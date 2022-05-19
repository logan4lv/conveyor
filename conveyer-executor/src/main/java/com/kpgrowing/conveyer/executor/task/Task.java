package com.kpgrowing.conveyer.executor.task;

public interface Task {
    public enum CompleteType{
        success,
        failure;
    }
    void start(TaskCompleteListener listener);

}
