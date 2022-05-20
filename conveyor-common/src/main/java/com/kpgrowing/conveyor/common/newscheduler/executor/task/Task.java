package com.kpgrowing.conveyor.common.newscheduler.executor.task;

import com.kpgrowing.conveyor.common.queue.Status;

public interface Task {
    Status run(TaskContext commandContext);
}
