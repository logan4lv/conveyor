package com.kpgrowing.conveyer.common.newscheduler.executor.task;

public interface TaskInterceptor {
    TaskInterceptor getNext();
    void setNext(TaskInterceptor taskInterceptor);
    void run(TaskContext taskContext, Task task);
}
