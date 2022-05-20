package com.kpgrowing.conveyor.common.newscheduler.executor.task;

import com.kpgrowing.conveyor.common.queue.Job;

public interface TaskConverter {
    Task convert(Job job);

    boolean isMatch(Job job);
}
