package com.kpgrowing.conveyor.common.newscheduler.scheduler;

import com.kpgrowing.conveyor.common.newscheduler.exception.RejectExistingGroupException;
import com.kpgrowing.conveyor.common.newscheduler.scheduler.zookeeper.ZkGroupJobQueue;
import com.kpgrowing.conveyor.common.newscheduler.scheduler.zookeeper.ZkGroupQueue;
import com.kpgrowing.conveyor.common.queue.Group;
import com.kpgrowing.conveyor.common.queue.Job;

/**
 * 锁由这里提供
 */
public class SchedulerJobManager {

    // TODO: 以下参数可以集中到配置类中
    private int waitingQueueLimit = -1;

    private final GroupQueue waitingQueue;
    private final GroupJobQueue outstandingQueue;
    private final GroupQueue completedQueue;


    public SchedulerJobManager() {
        this.waitingQueue = new ZkGroupQueue();
        this.outstandingQueue = new ZkGroupJobQueue();
        this.completedQueue = new ZkGroupQueue();
    }

    public void offer(Group group) {
        // write lock on outstandingQueue and waitingQueue
        if (outstandingQueue.contains(group)) {
            if (waitingQueueLimit <= 0) {
                throw new RejectExistingGroupException();
            }
            if (waitingQueue.size() >= waitingQueueLimit) {
                throw new RejectExistingGroupException();
            }
            waitingQueue.offer(group);
            return;
        }

        outstandingQueue.offer(group);
        // unlock
    }

    public void complete(Job job) {
        Group group = outstandingQueue.completeJob(job);
        if (group.isCompleted()) {
            complete(group);
        }
    }

    private void complete(Group group) {
        // write lock on outstandingQueue and waitingQueue
        if (outstandingQueue.remove(group) == null) {
            return;
        }
        Group waitingGroup = waitingQueue.remove(group);
        if (waitingGroup != null) {
            outstandingQueue.offer(waitingGroup);
        }
        // unlock

        // write lock on completedQueue
        completedQueue.offer(group);
        // unlock
    }

    public Group removeCompletedGroup(Group group) {
        // write lock on completedQueue
        Group completedGroup = completedQueue.remove(group);
        // unlock

        if (completedGroup != null) {
            // if group fail, use FailPolicy fail it
        }
        return completedGroup;
    }
}
