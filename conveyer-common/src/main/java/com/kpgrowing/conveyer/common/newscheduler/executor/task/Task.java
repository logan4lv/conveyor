package com.kpgrowing.conveyer.common.newscheduler.executor.task;

import com.kpgrowing.conveyer.common.queue.Status;

public interface Task {
    Status run(TaskContext commandContext);
}
