package com.kpgrowing.conveyer.common.newscheduler.scheduler.zookeeper;

import com.kpgrowing.conveyer.common.newscheduler.scheduler.GroupJobQueue;
import com.kpgrowing.conveyer.common.queue.Group;
import com.kpgrowing.conveyer.common.queue.Job;

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
