package com.kpgrowing.conveyor.common.worker;

import com.kpgrowing.conveyor.common.queue.BlockingGroupQueue;
import com.kpgrowing.conveyor.common.queue.Job;
import com.kpgrowing.conveyor.common.queue.Status;
import com.kpgrowing.conveyor.common.queue.zookeeper.ZKBlockingGroupQueue;
import com.kpgrowing.conveyor.common.support.ConveyorThread;
import com.kpgrowing.conveyor.common.worker.Task;
import com.kpgrowing.conveyor.common.worker.TaskCompleteListener;
import com.kpgrowing.conveyor.executor.task.SimpleJobConvert;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

@Slf4j
public class Worker extends ConveyorThread implements TaskCompleteListener {
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
        log.info("worker running");
        while(true) {
            while (semaphore.tryAcquire()) {
                try {
                    log.info("worker taking job..., semaphore available permits[{}]", semaphore.availablePermits());
                    Job job = queue.take();
                    log.info("worker take job[{}]", job.getGroupKey() + " - " + job.getKey());
                    pool.execute(() -> {
                        Task task = SimpleJobConvert.toTask(job);
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
