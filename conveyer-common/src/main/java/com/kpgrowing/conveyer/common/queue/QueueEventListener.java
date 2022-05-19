package com.kpgrowing.conveyer.common.queue;

public interface QueueEventListener {
    void process(QueueEvent event);
}