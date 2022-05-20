package com.kpgrowing.conveyor.common.worker;

public interface Task {
    public enum CompleteType{
        success,
        failure;
    }
    void start(TaskCompleteListener listener);

}
