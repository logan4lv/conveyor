package com.kpgrowing.conveyer.common.queue;

import com.alibaba.fastjson.JSON;
import com.kpgrowing.conveyer.common.queue.State;
import com.kpgrowing.conveyer.common.queue.Status;
import lombok.Data;

@Data
public class Job {
    private String groupKey;
    private int priority;
    private String key;

    private State state = State.none;
    private Status status = Status.none;

    public String toJSONString() {
        return JSON.toJSONString(this);
    }
}
