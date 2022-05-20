package com.kpgrowing.conveyor.common.newscheduler.executor.task;

public interface TaskInterceptor {
    TaskInterceptor getNext();
    void setNext(TaskInterceptor taskInterceptor);
    void run(TaskContext taskContext, Task task);
}
