package com.kpgrowing.conveyer.common.newscheduler.executor;

import com.kpgrowing.conveyer.common.queue.Job;

public interface BlockingJobQueue {
    void offer(Job job);
    Job take();
    void completeJob(Job job);
}
