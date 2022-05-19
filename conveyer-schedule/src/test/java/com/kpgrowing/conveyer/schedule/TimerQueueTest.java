package com.kpgrowing.conveyer.schedule;

import com.kpgrowing.conveyer.schedule.leader.TimerQueue;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;

@Slf4j
public class TimerQueueTest {

    @AllArgsConstructor
    @Data
    private static class QueueElem {
        private String name;
    }

    @Test
    public void testTimerQueue() {
        TimerQueue<QueueElem> queue = new TimerQueue<>(60 * 1000);
        queue.update(new QueueElem("1 minute"), "0 0/1 * * * *");
        queue.update(new QueueElem("2 minute"), "0 0/2 * * * *");

        queue.dump(new PrintWriter(System.out));

        Thread thread = new Thread(() -> {
            try {
                while (true) {
                    long waitTime = queue.getWaitTime();
                    log.info("waitTime is [{}]", waitTime);
                    if (waitTime > 0) {
                        log.info("sleep {}ms", waitTime);
                        Thread.sleep(waitTime);
                        continue;
                    }
                    log.info("End sleep, and poll queue");

                    for (QueueElem el : queue.poll()) {
                        log.info("trigger el[{}]", el.getName());
                    }

                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("timer tracker exited loop!");
        });

        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
