package com.kpgrowing.conveyer.common.newscheduler.executor.task;

import com.kpgrowing.conveyer.common.queue.Job;

public interface TaskConverter {
    Task convert(Job job);

    boolean isMatch(Job job);
}
