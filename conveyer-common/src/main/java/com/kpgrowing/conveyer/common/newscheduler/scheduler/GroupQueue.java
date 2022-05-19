package com.kpgrowing.conveyer.common.newscheduler.scheduler;

import com.kpgrowing.conveyer.common.queue.Group;

public interface GroupQueue {
    void offer(Group group);
    Group remove(Group group);
    boolean contains(Group group);
    int size();
}
