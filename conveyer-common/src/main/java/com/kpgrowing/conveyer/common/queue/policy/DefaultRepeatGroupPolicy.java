package com.kpgrowing.conveyer.common.queue.policy;

import com.kpgrowing.conveyer.common.queue.BlockingGroupQueue;
import com.kpgrowing.conveyer.common.queue.Group;
import com.kpgrowing.conveyer.common.queue.RepeatGroupPolicy;
import com.kpgrowing.conveyer.common.queue.exception.RepeatGroupRejectedException;

public class DefaultRepeatGroupPolicy implements RepeatGroupPolicy {

    @Override
    public void afterGroupRemove(BlockingGroupQueue queue, String groupKey) {
        // do nothing
    }

    public boolean beforeGroupOffer(BlockingGroupQueue queue, Group group) throws Exception {
        return queue.computeIfContains(group, () -> {
            throw new RepeatGroupRejectedException("repeat group, group key:" + group.getKey());
        });
    }
}
