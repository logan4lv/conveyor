package com.kpgrowing.conveyer.common.newscheduler;

import com.kpgrowing.conveyer.common.newscheduler.executor.BlockingJobQueue;
import com.kpgrowing.conveyer.common.newscheduler.executor.task.TaskConverters;
import com.kpgrowing.conveyer.common.newscheduler.executor.task.TaskInterceptor;
import com.kpgrowing.conveyer.common.newscheduler.executor.zookeeper.ZkBlockingJobQueue;
import com.kpgrowing.conveyer.common.newscheduler.registry.ControlCenter;
import com.kpgrowing.conveyer.common.queue.Job;
import com.kpgrowing.conveyer.common.support.LoganThread;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 启动时，先注册自己到Registry
 * 定时上报心跳
 * 监听 executor-job-queue 的事件，执行相应逻辑
 */
public class Executor extends LoganThread {

    private final BlockingJobQueue blockingJobQueue;
    private static final ExecutorService worksPool = Executors.newFixedThreadPool(20);
    private ControlCenter controlCenter;

    public Executor() {
        super("executor");
        blockingJobQueue = new ZkBlockingJobQueue();
    }

    @Override
    public void start() {
        // register self, keep alive for self.
        controlCenter.register(this);
        super.start();
    }

    @Override
    public void run() {
        while(true) {
            Job job = blockingJobQueue.take();
            worksPool.execute(() -> {
                // TODO: convert Job to Task with TaskConverters
                // TODO: run Task with TaskInterceptors. Contains complete job(maybe complete group) and notify event.
            });
        }
    }

    public void setTaskConverts(TaskConverters taskConverts) {

    }

    public void setTaskInterceptors(Collection<TaskInterceptor> interceptors) {

    }


}
