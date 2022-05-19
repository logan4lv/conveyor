package com.kpgrowing.conveyer.common.queue;

public interface RepeatGroupPolicy {
    void afterGroupRemove(BlockingGroupQueue queue, String groupKey);

    boolean beforeGroupOffer(BlockingGroupQueue queue, Group group) throws Exception;
}
