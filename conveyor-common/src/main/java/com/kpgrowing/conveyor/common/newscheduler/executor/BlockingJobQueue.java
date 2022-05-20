package com.kpgrowing.conveyor.common.newscheduler.executor;

import com.kpgrowing.conveyor.common.queue.Job;

public interface BlockingJobQueue {
    void offer(Job job);
    Job take();
    void completeJob(Job job);
}
