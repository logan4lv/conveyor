package com.kpgrowing.conveyor.schedule.leader;

public interface SchedulerLeaderLatch {
    void startLeaderElection(SchedulerLeaderLatchListener listener) throws Exception;
}
