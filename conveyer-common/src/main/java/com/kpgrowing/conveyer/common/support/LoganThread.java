package com.kpgrowing.conveyer.common.support;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class LoganThread extends Thread {

    private UncaughtExceptionHandler uncaughtExceptionalHandler = (t, e) -> handleException(t.getName(), e);

    public LoganThread(String threadName) {
        super(threadName);
        setUncaughtExceptionHandler(uncaughtExceptionalHandler);
    }

    protected void handleException(String thName, Throwable e) {
        log.warn("Exception occurred from thread {}", thName, e);
    }
}
