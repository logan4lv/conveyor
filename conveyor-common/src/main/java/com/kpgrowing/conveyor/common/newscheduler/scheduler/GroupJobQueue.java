package com.kpgrowing.conveyor.common.newscheduler.scheduler;

import com.kpgrowing.conveyor.common.queue.Group;
import com.kpgrowing.conveyor.common.queue.Job;

public interface GroupJobQueue extends GroupQueue {
    Job peekWithLock();
    Group completeJob(Job job);
}
