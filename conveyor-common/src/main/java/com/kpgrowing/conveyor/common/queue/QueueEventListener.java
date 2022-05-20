package com.kpgrowing.conveyor.common.queue;

public interface QueueEventListener {
    void process(QueueEvent event);
}