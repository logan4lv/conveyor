package com.kpgrowing.conveyor.common.queue.exception;

public class RepeatGroupRejectedException extends RuntimeException {
    public RepeatGroupRejectedException(String message) {
        super(message);
    }
}
