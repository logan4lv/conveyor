package com.kpgrowing.conveyer.common.queue;

import java.util.function.Supplier;

import static org.apache.curator.utils.ZKPaths.makePath;

/**
 * TODO: 分离出Provider + Consumer的模型，队外不提供队列使用
 */
public interface BlockingGroupQueue extends QueueEventFirer {

    boolean offer(Group g) throws Exception;

    void remove(String groupKey);

    boolean computeIfContains(Group g, Supplier<Void> compute) throws Exception;

    /**
     * blocking operation
     * two balance strategies:
     * 1.client balance use java semaphore to control the number of running jobs
     * 2.server balance use distributed solution, create the node instances, every client register itself on the node
     *
     * @return
     * @throws InterruptedException
     */
    Job take() throws Exception;

    void completeJob(Job job, Status status);

    void stopJob(String jobNode);

    void broadcastCompleteGroup();
}
