package com.kpgrowing.conveyer.executor;

import com.kpgrowing.conveyer.common.queue.BlockingGroupQueue;
import com.kpgrowing.conveyer.common.queue.Job;
import com.kpgrowing.conveyer.common.queue.Status;
import com.kpgrowing.conveyer.common.queue.zookeeper.ZKBlockingGroupQueue;
import com.kpgrowing.conveyer.common.support.LoganThread;
import com.kpgrowing.conveyer.executor.task.Task;
import com.kpgrowing.conveyer.executor.task.TaskCompleteListener;
import com.kpgrowing.conveyer.executor.task.TaskFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

@Slf4j
public class Worker extends LoganThread implements TaskCompleteListener {
    private static final ExecutorService pool = Executors.newFixedThreadPool(10);
    private static final Semaphore semaphore = new Semaphore(10);
    private BlockingGroupQueue queue;

    public Worker() {
        super("");
        queue = new ZKBlockingGroupQueue(null, null);
    }

    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    public void run() {
        log.info("job-executor running");
        while(true) {
            while (semaphore.tryAcquire()) {
                try {
                    log.info("job-executor taking job..., semaphore avaliable permits[{}]",
                            semaphore.availablePermits());
                    Job job = queue.take();
                    log.info("job-executor taked job[{}]", job.getGroupKey() + " - " + job.getKey());
                    pool.execute(() -> {
                        Task task = TaskFactory.produce(job);
                        task.start(this);
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void taskCompleted(Job job, Task.CompleteType type) {
        Status status = type == Task.CompleteType.success ? Status.success : Status.failure;
        queue.completeJob(job, status);
        semaphore.release();
        log.info("task Completed, job[{}]", (job.getGroupKey() + " - " + job.getKey()));
    }
}
