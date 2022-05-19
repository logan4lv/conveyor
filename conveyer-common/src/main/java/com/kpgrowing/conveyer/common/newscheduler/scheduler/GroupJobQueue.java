package com.kpgrowing.conveyer.common.newscheduler.scheduler;

import com.kpgrowing.conveyer.common.queue.Group;
import com.kpgrowing.conveyer.common.queue.Job;

public interface GroupJobQueue extends GroupQueue {
    Job peekWithLock();
    Group completeJob(Job job);
}
