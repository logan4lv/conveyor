package com.kpgrowing.conveyor.executor.task;

import com.kpgrowing.conveyor.common.queue.Job;

@FunctionalInterface
public interface TaskCompleteListener {
    void taskCompleted(Job job, Task.CompleteType type);
}
