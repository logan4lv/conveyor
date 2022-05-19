package com.kpgrowing.conveyer.common.queue;

/**
 * retry policy
 * 触发时机：任务删除 &任务失败 & 任务开启了重启
 * 策略：
 * 1.标识任务为重试任务，重试次数+1
 * 2.到达maxRetryTimes时，不再重试
 */
public interface RetryPolicy {
    void retry(BlockingGroupQueue queue, Group group);

    boolean isRetryable(Group group);
}
