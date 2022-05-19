package com.kpgrowing.conveyer.common.queue.policy;

import com.kpgrowing.conveyer.common.queue.BlockingGroupQueue;
import com.kpgrowing.conveyer.common.queue.Group;
import com.kpgrowing.conveyer.common.queue.RetryPolicy;

public class DefaultRetryPolicy implements RetryPolicy {
    @Override
    public void retry(BlockingGroupQueue queue, Group group) {
        if (isRetryable(group)) {

        }
        // do nothing
    }

    @Override
    public boolean isRetryable(Group group) {
        return false;
    }
}
