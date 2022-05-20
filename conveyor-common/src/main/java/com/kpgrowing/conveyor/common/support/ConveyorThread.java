package com.kpgrowing.conveyor.common.support;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConveyorThread extends Thread {

    private UncaughtExceptionHandler uncaughtExceptionalHandler = (t, e) -> handleException(t.getName(), e);

    public ConveyorThread(String threadName) {
        super(threadName);
        setUncaughtExceptionHandler(uncaughtExceptionalHandler);
    }

    protected void handleException(String thName, Throwable e) {
        log.warn("Exception occurred from thread {}", thName, e);
    }
}
