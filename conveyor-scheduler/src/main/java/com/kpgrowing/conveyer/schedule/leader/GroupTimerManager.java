package com.kpgrowing.conveyor.schedule.leader;

import com.kpgrowing.conveyor.schedule.Dispatcher;
import com.kpgrowing.conveyor.schedule.JobTestSender;
import com.kpgrowing.conveyor.common.queue.Group;
import com.kpgrowing.conveyor.common.support.ConveyorThread;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

@Slf4j
public class GroupTimerManager extends ConveyorThread {

    /**
     * just care removed config.
     */
    private static Map<String, Group> groupCache = new HashMap<>();

    private Timer syncGroupConfigsTimer;
    private volatile boolean isRunning = false;
    private TimerQueue<Group> timerQueue;
    private Dispatcher dispatcher;

    public GroupTimerManager(Dispatcher dispatcher) {
        super("group-timer-manager");
        timerQueue = new TimerQueue<>(60 * 1000);
        syncGroupConfigsTimer = new Timer();
        this.dispatcher = dispatcher;
    }

    public void start() {
        log.info("group-timer-manager starting...");
        log.info("set group-timer-manager running.");
        isRunning = true;

        log.info("initialize TimerQueue.");
        syncGroupConfigsAndUpdateTimerQueue();

//        log.info("set timer for sync group configs and update TimerQueue.");
//        syncGroupConfigsTimer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                syncGroupConfigsAndUpdateTimerQueue();
//            }
//        }, 0, 1000 * 60 * 5);

        super.start();
        log.info("job-timer-manager started.");
    }

    private void syncGroupConfigsAndUpdateTimerQueue() {
        log.info("begin sync group configs and update TimerQueue.");

        log.info("sync group configs.");
        Map<String, Group> newCache = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            Group group = JobTestSender.generateGroup();
            newCache.put(group.getKey(), group);
        }

        for (String newGroupKey : newCache.keySet()) {
            if (groupCache.containsKey(newGroupKey)) {
                groupCache.remove(newGroupKey);
            }
        }

        for (Group group : groupCache.values()) {
            log.info("group removed from TimerQueue, group:[{}]", group.getKey());
            timerQueue.remove(group);
        }

        groupCache = newCache;

        for (Group group : groupCache.values()) {
            log.info("update TimerQueue, group:[{}]", group.getKey());
            timerQueue.update(group, group.getCron());
        }
    }

    @Override
    public void run() {
        try {
            while (isRunning) {
                long waitTime = timerQueue.getWaitTime();
                if (waitTime > 0) {
                    log.info("wait time is {}ms, sleep.", waitTime);
                    Thread.sleep(waitTime);
                    continue;
                }
                log.info("wait time end, poll from timer queue");

                for (Group group : timerQueue.poll()) {
                    log.info("timer offer group [{}]", group.getKey());
                    this.dispatcher.offerGroup(group);
                }

            }
        } catch (InterruptedException e) {
            handleException(this.getName(), e);
        }
        log.info("job-timer-manager exited loop!");
    }


    public void shutdown() {
        isRunning = false;
        timerQueue = null;
        syncGroupConfigsTimer.cancel();
        syncGroupConfigsTimer.purge();
    }
}
