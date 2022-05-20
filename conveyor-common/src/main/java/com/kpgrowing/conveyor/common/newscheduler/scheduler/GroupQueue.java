package com.kpgrowing.conveyor.common.newscheduler.scheduler;

import com.kpgrowing.conveyor.common.queue.Group;

public interface GroupQueue {
    void offer(Group group);
    Group remove(Group group);
    boolean contains(Group group);
    int size();
}
