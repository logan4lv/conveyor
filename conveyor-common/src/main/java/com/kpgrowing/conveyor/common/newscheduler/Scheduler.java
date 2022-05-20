package com.kpgrowing.conveyor.common.newscheduler;

import com.kpgrowing.conveyor.common.newscheduler.exception.RejectExistingGroupException;
import com.kpgrowing.conveyor.common.queue.Group;
import com.kpgrowing.conveyor.common.queue.Job;

import java.util.Collection;

public class Scheduler {
    public void start() {

    }

    public void offer(Group group) throws RejectExistingGroupException {

    }

    public void stop(Job job) {

    }

    public void setGroupCompeteListener(GroupCompleteListener listener) {

    }

    interface GroupCompleteListener {
        void complete(Collection<Group> group);
    }
}
