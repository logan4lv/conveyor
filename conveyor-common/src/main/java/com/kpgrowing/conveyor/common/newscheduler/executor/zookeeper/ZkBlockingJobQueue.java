package com.kpgrowing.conveyor.common.newscheduler.executor.zookeeper;

import com.kpgrowing.conveyor.common.newscheduler.executor.BlockingJobQueue;
import com.kpgrowing.conveyor.common.queue.Job;

public class ZkBlockingJobQueue implements BlockingJobQueue {
    @Override
    public void offer(Job job) {

    }

    @Override
    public Job take() {
        return null;
    }

    @Override
    public void completeJob(Job job) {

    }
}
