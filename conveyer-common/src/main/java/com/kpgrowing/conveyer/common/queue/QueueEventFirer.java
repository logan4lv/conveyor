package com.kpgrowing.conveyer.common.queue;

public interface QueueEventFirer {
    void registerListener(QueueEventListener listener);

    void removeListener(QueueEventListener listener);

    void fireEvent(QueueEvent event);
}
