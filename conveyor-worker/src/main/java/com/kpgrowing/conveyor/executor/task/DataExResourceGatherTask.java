package com.kpgrowing.conveyor.executor.task;

import com.kpgrowing.conveyor.common.queue.Job;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataExResourceGatherTask implements Task {

    private final Job job;

    public DataExResourceGatherTask(Job job) {
        this.job = job;
    }

    @SneakyThrows
    @Override
    public void start(TaskCompleteListener listener) {
        log.info("task begin exec, job[{}]", (job.getGroupKey() + " - " + job.getKey()));
        Thread.sleep(5000);
        log.info("task   end exec, job[{}]", (job.getGroupKey() + " - " + job.getKey()));

        listener.taskCompleted(job, CompleteType.success);
    }

}
