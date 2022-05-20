package com.kpgrowing.conveyor.executor.task;

import com.kpgrowing.conveyor.common.worker.JobConvert;
import com.kpgrowing.conveyor.common.queue.Job;
import com.kpgrowing.conveyor.common.worker.Task;

public class SimpleJobConvert implements JobConvert {

    public Task toTask(Job job) {
        return new DataExResourceGatherTask(job);
    }
}
