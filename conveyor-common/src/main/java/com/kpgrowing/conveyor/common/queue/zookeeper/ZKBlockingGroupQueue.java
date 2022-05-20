package com.kpgrowing.conveyor.common.queue.zookeeper;

import com.alibaba.fastjson.JSON;
import com.kpgrowing.conveyor.common.queue.*;
import com.kpgrowing.conveyor.common.queue.exception.RepeatGroupRejectedException;
import com.kpgrowing.conveyor.common.queue.policy.DefaultRepeatGroupPolicy;
import com.kpgrowing.conveyor.common.queue.policy.DefaultRetryPolicy;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.kpgrowing.conveyor.common.queue.QueueEvent.Type.*;
import static org.apache.curator.utils.ZKPaths.getNodeFromPath;
import static org.apache.curator.utils.ZKPaths.makePath;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted;

/**
 * 锁设计
 * 1.读写锁
 * - 同步Group队列的操作，note：部分操作只有leader才会使用，例：删除Group、
 * - 读
 * - 写：删除Group、添加Group
 * 2.Group锁
 * - 用来锁组信息的更新，同步更新
 * 3.Job锁
 * - 用来锁定任务，然后再执行。临时节点，程序挂了后，其他程序可以重新执行。所以任务执行需要设计为可以幂等。
 */
@Slf4j
public class ZKBlockingGroupQueue extends AbstractZKQueue implements BlockingGroupQueue {

    private RepeatGroupPolicy repeatGroupPolicy;
    private RetryPolicy retryPolicy;
    private CuratorFramework client;
    private List<QueueEventListener> listeners;

    private final ReentrantLock lock;
    private final Condition notEmpty;

    private final InterProcessReadWriteLock rwLock;
    private CuratorWatcher runningJobMonitor;
    private final Set<String> monitoredRunningJobPaths = new HashSet<>();

    public ZKBlockingGroupQueue(RepeatGroupPolicy repeatGroupPolicy, RetryPolicy retryPolicy) {

        client = CuratorFrameworkFactory.builder()
                .namespace(NAMESPACE)
                .retryPolicy(new RetryForever(3000))
                .connectString("127.0.0.1")
                .build();
        client.start();

        this.lock = new ReentrantLock();
        this.notEmpty = this.lock.newCondition();
        rwLock = new InterProcessReadWriteLock(client, PATH_LOCK_RW);

        if ((this.repeatGroupPolicy = repeatGroupPolicy) == null) {
            this.repeatGroupPolicy = new DefaultRepeatGroupPolicy();
        }

        if ((this.retryPolicy = retryPolicy) == null) {
            this.retryPolicy = new DefaultRetryPolicy();
        }

        initNotifyWatchers();
        mkPaths(PATH_GROUPS, PATH_NOTIFY_ADD, PATH_NOTIFY_COMPLETE, PATH_NOTIFY_JOB_STOP);
        initWatcher();
    }

    private void mkPaths(String... paths) {
        try {
            if (paths != null) {
                for (String path : paths) {
                    client.create()
                            .creatingParentsIfNeeded()
                            .withMode(CreateMode.PERSISTENT)
                            .inBackground()
                            .forPath(path);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initWatcher() {
        runningJobMonitor = new CuratorWatcher() {
            @Override
            public void process(WatchedEvent event) throws Exception {
                if (event.getType() == NodeDeleted) {
                    final ReentrantLock slock = lock;
                    slock.lockInterruptibly();
                    try {
                        notEmpty.signal();
                    } finally {
                        slock.unlock();
                        monitoredRunningJobPaths.remove(event.getPath());
                    }
                }
            }
        };
    }

    private void initNotifyWatchers() {
        try {
            client.watchers()
                    .add()
                    .withMode(AddWatchMode.PERSISTENT)
                    .usingWatcher(new CuratorWatcher() {
                        @Override
                        public void process(WatchedEvent event) throws Exception {
                            if (event.getType() == NodeDataChanged) {
                                List<Group> completedGroups = listCompletedGroup();
                                fireEvent(
                                        QueueEvent
                                                .builder()
                                                .type(GROUP_COMPLETE)
                                                .groups(completedGroups)
                                                .build()
                                );
                            }
                        }
                    }).forPath(PATH_NOTIFY_COMPLETE);
            client.watchers()
                    .add()
                    .withMode(AddWatchMode.PERSISTENT)
                    .usingWatcher(new CuratorWatcher() {
                        @Override
                        public void process(WatchedEvent event) throws Exception {
                            if (event.getType() == NodeDataChanged) {
                                fireEvent(QueueEvent.builder().type(GROUP_ADD).build());
                                final ReentrantLock slock = lock;
                                slock.lockInterruptibly();
                                try {
                                    notEmpty.signal();
                                } finally {
                                    slock.unlock();
                                }
                            }
                        }
                    }).forPath(PATH_NOTIFY_ADD);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean offer(Group g) throws Exception {
        log.info("offer group, group key:[{}]", g.getKey());
        String groupPath;
        InterProcessMutex wLock = rwLock.writeLock();
        log.info("acquire-w-lock[offer][before] - group[{}]", g.getKey());
        while (!wLock.acquire(500, TimeUnit.MILLISECONDS)) ;
        log.info("acquire-w-lock[offer][ after] - group[{}]", g.getKey());

        try {
            if (repeatGroupPolicy.beforeGroupOffer(this, g)) {
                log.info("exists repeat group, group key:[{}]", g.getKey());
                return true;
            }

            groupPath = client
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                    .forPath(makePath(PATH_GROUPS, g.getKey() + GROUP_SEQUENTIAL_SEPARATOR),
                            g.toJSONString().getBytes());
            g.getJobs().stream()
                    .sorted(Comparator.comparingInt(Job::getPriority))
                    .forEach(j -> {
                        try {
                            client.create()
                                    .creatingParentsIfNeeded()
                                    .withMode(CreateMode.PERSISTENT)
                                    .forPath(makePath(groupPath, j.getKey()), j.toJSONString().getBytes());
                        } catch (Exception e) {
                            log.error("add job occur error, msg: {}", e.getMessage());
                            throw new RuntimeException(e);
                        }
                    });
        } catch (RepeatGroupRejectedException e) {
            log.error("reject repeat group.");
            throw e;
        } catch (Exception e) {
            log.error("add group occur error, msg: {}", e.getMessage());
            throw new Exception("add group occur error, msg:" + e.getMessage());
        } finally {
            wLock.release();
            log.info("released-w-lock[offer] - group[{}]", g.getKey());
        }
        doNotify(GROUP_ADD, getNodeFromPath(groupPath), null);
        return true;
    }

    @Override
    public void remove(String groupKey) {
        InterProcessMutex wLock = rwLock.writeLock();
        log.info("acquire-w-lock[remove][before] - group[{}]", groupKey);
        try {
            while (!wLock.acquire(500, TimeUnit.MILLISECONDS)) ;
        } catch (Exception e) {
            log.error("remove group occur error when acquire write lock, msg:" + e.getMessage());
            return;
        }
        log.info("acquire-w-lock[remove][ after] - group[{}]", groupKey);
        log.info("remove group key [{}]", groupKey);
        try {
            String node = getGroupNodeByKey(groupKey);
            if (node != null) {
                log.info("remove group node {}", node);
                client.delete()
                        .guaranteed()
                        .deletingChildrenIfNeeded()
                        .forPath(makePath(PATH_GROUPS, node));
                client.delete()
                        .guaranteed()
                        .deletingChildrenIfNeeded()
                        .forPath(makePath(PATH_NOTIFY_COMPLETE, node));

                // TODO: perform delete action, try event fire
                this.repeatGroupPolicy.afterGroupRemove(this, groupKey);

                // TODO: perform retry strategy, try event fire
//                retryPolicy.retry(this, peek(groupKey));
            }
        } catch (Exception e) {
            log.error("delete group occur error, msg: {}", e.getMessage());
        } finally {
            try {
                wLock.release();
            } catch (Exception e) {
                log.error("remove group occur error when release write lock, msg:" + e.getMessage());
            }
            log.info("released-w-lock[remove] - group[{}]", groupKey);
        }
    }

    /**
     * await for empty jobs
     * 唤醒条件
     * 1.组添加事件中
     */
    @Override
    public Job take() throws Exception {
        final ReentrantLock slock = this.lock;
        slock.lockInterruptibly();
        Job job = null;
        try {
            while ((job = peekWithLock()) == null) {
                notEmpty.await();
            }

            // do balance
        } finally {
            slock.unlock();
        }
        return job;
    }

    private Group peek(String groupKey) throws Exception {
        Group group;

        log.info("peeking group by key[{}]", groupKey);
        String groupNode = null;
        List<String> groupNodes = client.getChildren().forPath(PATH_GROUPS);
        for (String node : groupNodes) {
            if (node.indexOf(groupKey + GROUP_SEQUENTIAL_SEPARATOR) == 0) {
                groupNode = node;
                break;
            }
        }
        if (groupNode == null) {
            log.info("not found");
            return null;
        }
        log.info("peeked group[{}]", groupNode);
        String groupPath = makePath(PATH_GROUPS, groupNode);
        byte[] groupBytes = client.getData().forPath(groupPath);
        group = JSON.parseObject(groupBytes, Group.class);
        group.setJobs(new ArrayList<>());

        List<String> jobNodes = client.getChildren().forPath(groupPath);
        for (String jobNode : jobNodes) {
            byte[] jobBytes = client.getData().forPath(makePath(groupPath, jobNode));
            Job job = JSON.parseObject(jobBytes, Job.class);
            group.getJobs().add(job);
        }
        return group;
    }

    private Job peekWithLock() throws Exception {
        Map<String, Job> runningJobPaths = new HashMap<>();
        Map<String, List<Job>> allGroups = getSortedGroupMap();

        for (String groupNode : allGroups.keySet()) {
            List<Job> jobs = allGroups.get(groupNode);
            Map<Integer, List<Job>> priorityWithJobs = jobs.stream().collect(Collectors.groupingBy(Job::getPriority));
            Integer[] jobPrioritySet = priorityWithJobs.keySet().toArray(new Integer[priorityWithJobs.size()]);
            Arrays.sort(jobPrioritySet);
            boolean prevPriorityIsRunning = false;
            for (int i : jobPrioritySet) {
                if (prevPriorityIsRunning) {
                    log.info("prev priority jobs is running, skip current priority.");
                    break;
                }

                boolean currentPriorityIsRunning = false;
                for (Job job : priorityWithJobs.get(i)) {
                    String jobPath = makePath(PATH_GROUPS, groupNode, job.getKey());
                    switch (job.getState()) {
                        case none:
                            if (!prevPriorityIsRunning) {
                                boolean lockSuccess = acquireJobLock(jobPath);
                                if (lockSuccess) {
                                    log.info("acquire lock for none state job successfully. return job.");
                                    job.setState(State.running);
                                    client.setData().inBackground().forPath(jobPath, job.toJSONString().getBytes());
                                    return job;
                                }
                            }
                            break;
                        case running:
                            currentPriorityIsRunning = true;
                            runningJobPaths.put(jobPath, job);
                        case complete:
                    }
                }
                prevPriorityIsRunning = currentPriorityIsRunning;
            }
        }

        if (!runningJobPaths.isEmpty()) {
            log.info("watch prev jobs until completed and then await taking.");
            try {
                for (String jobPath : runningJobPaths.keySet()) {

                    if (runningJobLostLock(jobPath)) {
                        log.info("running jobPath[{}] lost lock, reacquire its lock.", jobPath);
                        boolean lockSuccess = acquireJobLock(jobPath);
                        if (lockSuccess) {
                            log.info("reacquire lock for running state job successfully. return job.");
                            return runningJobPaths.get(jobPath);
                        }
                    }

                    if (!monitoredRunningJobPaths.contains(jobPath)) {
                        client.watchers()
                                .add()
                                .withMode(AddWatchMode.PERSISTENT)
                                .usingWatcher(runningJobMonitor)
                                .forPath(makePath(jobPath, NODE_LOCK_JOB));
                        monitoredRunningJobPaths.add(jobPath);
                    }
                }
            } catch (Exception e) {
                log.error("error occured when set watcher for running job.");
            }
        }

        return null;
    }

    private Map<String, List<Job>> getSortedGroupMap() {
        Map<String, List<Job>> groupNodeWithJobs = new LinkedHashMap<>();
        InterProcessMutex rLock = rwLock.readLock();
        log.info("acquire-r-lock[getSortedGroupMap][before]");
        try {
            while (!rLock.acquire(500, TimeUnit.MILLISECONDS)) ;
        } catch (Exception e) {
            log.error("take job occur error when acquire read lock, msg:" + e.getMessage());
            return new LinkedHashMap<>();
        }
        log.info("acquire-r-lock[getSortedGroupMap][ after]");

        try {
            List<String> groupNodes = client.getChildren().forPath(PATH_GROUPS);
            List<String> sortedGroupNodes = sortGroupNodes(groupNodes);
            for (String groupNode : sortedGroupNodes) {
                List<String> jobNodes = client.getChildren().forPath(makePath(PATH_GROUPS, groupNode));
                List<Job> jobs = new ArrayList<>();
                for (String jobNode : jobNodes) {
                    jobs.add(getJobForPath(groupNode, jobNode));
                }
                groupNodeWithJobs.put(groupNode, jobs);
            }
        } catch (Exception e) {
            log.error("error occured, msg: " + e.getMessage());
            return new LinkedHashMap<>();
        } finally {
            try {
                rLock.release();
            } catch (Exception e) {
                log.error("take job occur error when release read lock, msg:" + e.getMessage());
            }
            log.info("released-r-lock[getSortedGroupMap]");
        }

        return groupNodeWithJobs;
    }

    private boolean acquireJobLock(String jobPath) {
        try {
            String lockPath = client.create()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(makePath(jobPath, NODE_LOCK_JOB));
            log.info("lock success. jobPath:[{}]", jobPath);
            return true;
        } catch (Exception e) {
            log.warn("lock fail. jobPath:[{}]", jobPath);
            return false;
        }
    }

    private boolean runningJobLostLock(String jobPath) {

        List<String> nodes = ZKPaths.split(jobPath);
        String groupNode = nodes.get(nodes.size() - 2);
        String seqSuffix = ZKPaths.extractSequentialSuffix(groupNode);
        String groupKey = groupNode.replace(GROUP_SEQUENTIAL_SEPARATOR + seqSuffix, "");

        InterProcessMutex lock = new InterProcessMutex(client, makePath(NODE_LOCK_GROUP, groupKey));
        try {
            while (!lock.acquire(500, TimeUnit.MILLISECONDS)) ;
        } catch (Exception e) {
            log.error("complete job occur error when acquire lock, msg: " + e.getMessage());
        }

        try {
            byte[] jobBytes = client.getData().forPath(jobPath);
            Job parsedJob = JSON.parseObject(jobBytes, Job.class);

            if (parsedJob.getState() == State.running) {
                if (client.checkExists().forPath(makePath(jobPath, "lock")) == null) {
                    return true;
                }
            }

        } catch (Exception e) {
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                log.error("complete job occur error when release lock, msg: " + e.getMessage());
            }
        }

        return false;
    }

    @Override
    public void completeJob(Job job, Status status) {
        InterProcessMutex lock = new InterProcessMutex(client, makePath(NODE_LOCK_GROUP, job.getGroupKey()));
        try {
            while (!lock.acquire(500, TimeUnit.MILLISECONDS)) ;
        } catch (Exception e) {
            log.error("complete job occur error when acquire lock, msg: " + e.getMessage());
        }

        try {
            String groupNode = getGroupNodeByKey(job.getGroupKey());
            log.info("completeJob: groupNode:[{}], groupKey:[{}]", groupNode, job.getGroupKey());
            String pathJob = makePath(PATH_GROUPS, groupNode, job.getKey());
            String pathGroup = makePath(PATH_GROUPS, groupNode);

            byte[] jobBytes = client.getData().forPath(pathJob);
            Job parsedJob = JSON.parseObject(jobBytes, Job.class);
            parsedJob.setStatus(status);
            parsedJob.setState(State.complete);
            client.setData().forPath(pathJob, parsedJob.toJSONString().getBytes());

            client.delete().inBackground().forPath(makePath(pathJob, "lock"));

            byte[] groupBytes = client.getData().forPath(pathGroup);
            Group parsedGroup = JSON.parseObject(groupBytes, Group.class);
            parsedGroup.updateByJobStatus(status);
            client.setData().forPath(pathGroup, parsedGroup.toJSONString().getBytes());

            if (parsedGroup.getState() == State.complete) {
                doNotify(GROUP_COMPLETE, groupNode, null);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                log.error("complete job occur error when release lock, msg: " + e.getMessage());
            }
        }
    }

    private List<Group> listCompletedGroup() {
        List<Group> groups = new ArrayList<>();
        try {
            List<String> completedGroups = client.getChildren().forPath(PATH_NOTIFY_COMPLETE);
            for (String groupNode : completedGroups) {
                byte[] groupBytes = client.getData().forPath(makePath(PATH_GROUPS, groupNode));
                Group parsedGroup = JSON.parseObject(groupBytes, Group.class);
                groups.add(parsedGroup);
            }
        } catch (Exception e) {
            log.info("error occured when get completed group, msg: " + e.getMessage());
        }

        return groups;
    }

    private Job getJobForPath(String groupNode, String jobNode) throws Exception {
        byte[] bytes = client.getData().forPath(makePath(PATH_GROUPS, groupNode, jobNode));
        Job job = JSON.parseObject(new String(bytes), Job.class);
        return job;
    }

    @SneakyThrows
    public void doNotify(QueueEvent.Type type, String groupNode, String jobNode) {
        switch (type) {
            case GROUP_ADD:
                client.setData().forPath(PATH_NOTIFY_ADD);
                return;
            case GROUP_COMPLETE:
                if (StringUtils.isNotBlank(groupNode)) {
                    client.create()
                            .creatingParentsIfNeeded()
                            .withMode(CreateMode.PERSISTENT)
                            .inBackground()
                            .forPath(makePath(PATH_NOTIFY_COMPLETE, groupNode));
                }
                client.setData().forPath(PATH_NOTIFY_COMPLETE);
            case JOB_STOP:
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .inBackground()
                        .forPath(makePath(PATH_NOTIFY_JOB_STOP, jobNode));
                client.setData().forPath(PATH_NOTIFY_JOB_STOP);
                return;
        }
    }

    @Override
    public boolean computeIfContains(Group g, Supplier<Void> compute) throws Exception {
        InterProcessMutex rLock = rwLock.readLock();
        log.info("acquire-r-lock[computeIfContains][before] - group[{}]", g.getKey());
        while (!rLock.acquire(500, TimeUnit.MILLISECONDS)) ;
        log.info("acquire-r-lock[computeIfContains][ after] - group[{}]", g.getKey());
        try {
            String groupNode = getGroupNodeByKey(g.getKey());
            if (StringUtils.isNotBlank(groupNode)) {
                compute.get();
                return true;
            }
        } finally {
            rLock.release();
            log.info("released-r-lock[computeIfContains] - group[{}]", g.getKey());
        }

        return false;
    }

    private String getGroupNodeByKey(String key) {
        Map<String, List<Job>> groupNodeWithJobs = new LinkedHashMap<>();
        try {
            List<String> groupNodes = client.getChildren().forPath(PATH_GROUPS);
            for (String groupNode : groupNodes) {
                if (groupNode.indexOf(key + GROUP_SEQUENTIAL_SEPARATOR) == 0) {
                    return groupNode;
                }
            }
        } catch (Exception e) {
            log.error("delete group occur error, msg: {}", e.getMessage());
        }
        return null;
    }

    @Override
    public void registerListener(QueueEventListener listener) {
        if (this.listeners == null) {
            synchronized (this) {
                if (this.listeners == null) {
                    this.listeners = new ArrayList<>();
                }
            }
        }
        this.listeners.add(listener);
    }

    @Override
    public void removeListener(QueueEventListener listener) {
        if (this.listeners != null) {
            this.listeners.remove(listener);
        }
    }

    @Override
    public void fireEvent(QueueEvent event) {
        if (this.listeners != null) {
            for (QueueEventListener listener : this.listeners) {
                listener.process(event);
            }
        }
    }

    @Override
    public void stopJob(String jobNode) {
        doNotify(JOB_STOP, null, jobNode);
    }

    @Override
    public void broadcastCompleteGroup() {
        doNotify(GROUP_COMPLETE, null, null);
    }
}
