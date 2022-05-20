package com.kpgrowing.conveyor.common.newscheduler.scheduler.zookeeper;

import com.kpgrowing.conveyor.common.newscheduler.scheduler.GroupJobQueue;
import com.kpgrowing.conveyor.common.queue.Group;
import com.kpgrowing.conveyor.common.queue.Job;

public class ZkGroupJobQueue extends ZkGroupQueue implements GroupJobQueue {
    @Override
    public Job peekWithLock() {
        return null;
    }

    @Override
    public Group completeJob(Job job) {
        return null;
    }
}
