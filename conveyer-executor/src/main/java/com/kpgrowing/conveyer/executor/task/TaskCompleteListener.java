package com.kpgrowing.conveyer.executor.task;

import com.kpgrowing.conveyer.common.queue.Job;

@FunctionalInterface
public interface TaskCompleteListener {
    void taskCompleted(Job job, Task.CompleteType type);
}
