package com.kpgrowing.conveyer.common.queue.zookeeper;

import com.alibaba.fastjson.JSON;
import com.kpgrowing.conveyer.common.queue.Group;
import com.kpgrowing.conveyer.common.queue.Job;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.curator.utils.ZKPaths.makePath;

@Slf4j
public class ZKWaitingGroupQueue extends AbstractZKQueue {

    private final CuratorFramework client;

    public ZKWaitingGroupQueue() {
        this.client = CuratorFrameworkFactory.builder()
                .namespace(NAMESPACE)
                .retryPolicy(new RetryForever(3000))
                .connectString("127.0.0.1")
                .build();
        client.start();
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .inBackground()
                    .forPath(PATH_WAITING_GROUPS);
        } catch (Exception e) {
            log.error("waiting queue create groups path occured error, msg:" + e.getMessage());
        }
    }

    public void offer(Group g) throws Exception {
        InterProcessMutex lock = new InterProcessMutex(client, PATH_LOCK_WAITING_GROUPS);
        log.info("waiting-group - acquire-lock [offer] [before] - [{}]", g.getKey());
        while (!lock.acquire(500, TimeUnit.MILLISECONDS)) ;
        log.info("waiting-group - acquire-lock [offer] [ after] - [{}]", g.getKey());
        try {
            String groupPath = client
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                    .forPath(makePath(PATH_WAITING_GROUPS, g.getKey() + GROUP_SEQUENTIAL_SEPARATOR),
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
        } finally {
            lock.release();
            log.info("waiting-group - released-lock [offer] - [{}]", g.getKey());
        }
    }

    public Group poll(String groupKey) throws Exception {
        InterProcessMutex lock = new InterProcessMutex(client, PATH_LOCK_WAITING_GROUPS);
        log.info("waiting-group - acquire-lock [poll] [before] - [{}]", groupKey);
        while (!lock.acquire(500, TimeUnit.MILLISECONDS)) ;
        log.info("waiting-group - acquire-lock [poll] [ after] - [{}]", groupKey);
        try {
            Group group;

            log.info("get waiting group node by key[{}]", groupKey);
            String groupNode = null;
            List<String> groupNodes = client.getChildren().forPath(PATH_WAITING_GROUPS);
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
            log.info("found group node [{}]", groupNode);

            String groupPath = makePath(PATH_WAITING_GROUPS, groupNode);
            byte[] groupBytes = client.getData().forPath(groupPath);
            group = JSON.parseObject(groupBytes, Group.class);
            group.setJobs(new ArrayList<>());

            List<String> jobNodes = client.getChildren().forPath(groupPath);
            for (String jobNode : jobNodes) {
                byte[] jobBytes = client.getData().forPath(makePath(groupPath, jobNode));
                Job job = JSON.parseObject(jobBytes, Job.class);
                group.getJobs().add(job);
            }
            client.delete().guaranteed().deletingChildrenIfNeeded().forPath(groupPath);
            return group;
        } finally {
            lock.release();
            log.info("waiting-group - released-lock [poll] - [{}]", groupKey);
        }
    }

    public List<Group> pollAll() throws Exception {
        InterProcessMutex lock = new InterProcessMutex(client, PATH_LOCK_WAITING_GROUPS);
        while (!lock.acquire(500, TimeUnit.MILLISECONDS)) ;
        List<Group> groups = new LinkedList<>();

        try {
            log.info("get all waiting group");
            List<String> groupNodes = client.getChildren().forPath(PATH_WAITING_GROUPS);

            if (groupNodes.isEmpty()) {
                return groups;
            }

            for (String groupNode : groupNodes) {
                String groupPath = makePath(PATH_WAITING_GROUPS, groupNode);
                byte[] groupBytes = client.getData().forPath(groupPath);
                Group group = JSON.parseObject(groupBytes, Group.class);
                group.setJobs(new ArrayList<>());

                List<String> jobNodes = client.getChildren().forPath(groupPath);
                for (String jobNode : jobNodes) {
                    byte[] jobBytes = client.getData().forPath(makePath(groupPath, jobNode));
                    Job job = JSON.parseObject(jobBytes, Job.class);
                    group.getJobs().add(job);
                }
                groups.add(group);
                client.delete().guaranteed().deletingChildrenIfNeeded().forPath(groupPath);
            }
        } finally {
            lock.release();
        }

        return groups;
    }
}
