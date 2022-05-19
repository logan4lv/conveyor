package com.kpgrowing.conveyer.executor.task;

import com.kpgrowing.conveyer.common.queue.Job;

public class TaskFactory {

    public static Task produce(Job job) {
        return new DataExResourceGatherTask(job);
    }
}
