package com.kpgrowing.conveyor.executor.task;

import com.kpgrowing.conveyor.common.queue.Job;

public class TaskFactory {

    public static Task produce(Job job) {
        return new DataExResourceGatherTask(job);
    }
}
