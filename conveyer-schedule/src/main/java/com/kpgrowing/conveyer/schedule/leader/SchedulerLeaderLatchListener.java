package com.kpgrowing.conveyer.schedule.leader;

public interface SchedulerLeaderLatchListener {
    void isLeader();

    void notLeader();
}
