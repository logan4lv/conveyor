package com.kpgrowing.conveyor.common.worker;

import com.kpgrowing.conveyor.common.newscheduler.executor.task.Task;
import com.kpgrowing.conveyor.common.queue.Job;

public interface JobConvert {
    Task toTask(Job job);

    boolean isMatch(Job job);
}
