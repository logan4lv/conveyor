package com.kpgrowing.conveyer.schedule.leader;

public interface SchedulerLeaderLatch {
    void startLeaderElection(SchedulerLeaderLatchListener listener) throws Exception;
}
