package com.kpgrowing.conveyor.common.queue.policy;

import com.kpgrowing.conveyor.common.queue.BlockingGroupQueue;
import com.kpgrowing.conveyor.common.queue.Group;
import com.kpgrowing.conveyor.common.queue.RepeatGroupPolicy;
import com.kpgrowing.conveyor.common.queue.zookeeper.ZKWaitingGroupQueue;
import lombok.extern.slf4j.Slf4j;

/**
 * leader:
 * 1.启动时，将waiting queue的group重新offer一遍
 * 2.queue删除操作后从waiting queue中offer
 * waiting queue的重复任务策略
 * 1.识别重复任务
 * 2.重复任务入到waiting queue
 */
@Slf4j
public class WaitingRepeatGroupPolicy implements RepeatGroupPolicy {

    private final ZKWaitingGroupQueue waitingQueue;

    public WaitingRepeatGroupPolicy() {
        waitingQueue = new ZKWaitingGroupQueue();
    }

    @Override
    public void afterGroupRemove(BlockingGroupQueue queue, String groupKey) {
        log.info("poll group from waiting queue and offer it");
        try {
            Group group = waitingQueue.poll(groupKey);
            if (group != null) {
                log.info("found waiting group:[{}], offer it", group.getKey());
                queue.offer(group);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean beforeGroupOffer(BlockingGroupQueue queue, Group group) throws Exception {
        return queue.computeIfContains(group, () -> {
            try {
                waitingQueue.offer(group);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
    }
}
