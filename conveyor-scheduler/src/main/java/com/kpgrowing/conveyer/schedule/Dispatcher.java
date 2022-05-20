package com.kpgrowing.conveyor.schedule;

import com.kpgrowing.conveyor.common.queue.BlockingGroupQueue;
import com.kpgrowing.conveyor.common.queue.Group;
import com.kpgrowing.conveyor.common.queue.QueueEvent;
import com.kpgrowing.conveyor.common.queue.QueueEventListener;
import com.kpgrowing.conveyor.common.queue.policy.DefaultRetryPolicy;
import com.kpgrowing.conveyor.common.queue.policy.WaitingRepeatGroupPolicy;
import com.kpgrowing.conveyor.common.queue.zookeeper.ZKBlockingGroupQueue;
import com.kpgrowing.conveyor.common.support.ConveyorThread;
import com.kpgrowing.conveyor.schedule.leader.GroupTimerManager;
import com.kpgrowing.conveyor.schedule.leader.SchedulerLeaderLatch;
import com.kpgrowing.conveyor.schedule.leader.SchedulerLeaderLatchListener;
import com.kpgrowing.conveyor.schedule.zookeeper.ZookeeperSchedulerLeaderLatch;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.kpgrowing.conveyor.common.queue.QueueEvent.Type.GROUP_COMPLETE;

/**
 * 调度程序入口
 * 1.创建分布式任务队列
 * 2.启动leader选举，成为leader后，启动leader
 * 3.启动任务接收线程
 * <p>
 * leader:
 * 1.启动任务定时器，开启轮询扫描更新任务配置
 * 2.监听任务完成事件，完成相应任务
 */
@Slf4j
public class Dispatcher implements SchedulerLeaderLatchListener {

    private final BlockingQueue<Group> recvQueue;

    private SchedulerLeaderLatch schedulerLeaderLatch;
    private Leader leader;
    private volatile boolean isLeader;
    private final BlockingGroupQueue queue;
    private final JobSender jobSender;
    private Dispatcher self = this;

    public Dispatcher() {
        queue = new ZKBlockingGroupQueue(new WaitingRepeatGroupPolicy(), new DefaultRetryPolicy());
        recvQueue = new LinkedBlockingQueue();
        schedulerLeaderLatch = new ZookeeperSchedulerLeaderLatch();
        jobSender = new JobSender();
    }

    public void start() throws Exception {
        schedulerLeaderLatch.startLeaderElection(this);
        jobSender.start();
    }

    @Override
    public void isLeader() {
        isLeader = true;
        leader = new Leader();
        queue.registerListener(leader);
        leader.start();
    }

    @Override
    public void notLeader() {
        isLeader = false;
        queue.removeListener(leader);
        leader.shutdown();
        leader = null;
    }

    public void offerGroup(Group group) {
        recvQueue.offer(group);
    }

    private class JobSender extends ConveyorThread {

        public JobSender() {
            super("job-receiver");
        }

        @SneakyThrows
        @Override
        public void run() {
            while (true) {
                log.info("job-sender taking...");
                Group group = recvQueue.take();
                log.info("job-sender receive a group[{}] and offer it to distributed queue.", group.getKey());
                queue.offer(group);
            }
        }
    }

    private class Leader implements QueueEventListener {

        private volatile boolean shutdown = true;
        private final GroupTimerManager groupTimerManager;

        public Leader() {
            groupTimerManager = new GroupTimerManager(self);
        }

        public void start() {
            shutdown = false;
            groupTimerManager.start();
            queue.broadcastCompleteGroup();
            log.info("scheudler leader started.");
        }

        public void shutdown() {
            shutdown = true;
            groupTimerManager.shutdown();
        }

        @Override
        public void process(QueueEvent event) {
            if (event.getType() == GROUP_COMPLETE) {
                List<Group> groups = event.getGroups();
                if (!CollectionUtils.isEmpty(groups)) {
                    for (Group group : groups) {
                        if (shutdown) {
                            return;
                        }

                        switch (group.getStatus()) {
                            case success:
                                break;
                            case failure:
                                break;
                        }

                        log.info("complete group[{}] and remove it.", group.getKey());
                        queue.remove(group.getKey());
                    }
                }
            }
        }
    }
}
