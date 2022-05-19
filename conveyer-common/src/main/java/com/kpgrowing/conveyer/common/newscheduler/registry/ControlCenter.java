package com.kpgrowing.conveyer.common.newscheduler.registry;

import com.kpgrowing.conveyer.common.newscheduler.Executor;

/**
 * Hold Executors. (Scheduler and Executor hold ControlCenter)
 * Manage event notification.
 * Important events
 * executor trigger
 *  - complete job      notify all schedulers to complete job and offer job to executor-job-queue.
 * scheduler trigger
 *  - offer group       notify all schedulers to offer job to executor-job-queue.
 *  - peek job          notify the selected executor to take job.
 *  - complete group    notify all schedulers to complete group.
 * control center trigger
 *  - executor offline  notify all schedulers to unlock the job and reset the state. delete executor register.
 *  - executor online   notify all schedulers to add the executor to executorHandlers.
 */
public interface ControlCenter {
    void register(Executor executor);
}
