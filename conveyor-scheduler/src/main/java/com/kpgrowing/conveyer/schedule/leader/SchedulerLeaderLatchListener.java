package com.kpgrowing.conveyor.schedule.leader;

public interface SchedulerLeaderLatchListener {
    void isLeader();

    void notLeader();
}
