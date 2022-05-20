package com.kpgrowing.conveyor.common.newscheduler.scheduler.zookeeper;

import com.kpgrowing.conveyor.common.newscheduler.scheduler.GroupQueue;
import com.kpgrowing.conveyor.common.queue.Group;

public class ZkGroupQueue implements GroupQueue {
    @Override
    public void offer(Group group) {

    }

    @Override
    public Group remove(Group group) {
        return null;
    }

    @Override
    public boolean contains(Group group) {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }
}
