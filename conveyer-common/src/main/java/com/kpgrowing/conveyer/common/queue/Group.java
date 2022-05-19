package com.kpgrowing.conveyer.common.queue;

import com.alibaba.fastjson.JSON;
import lombok.Data;

import java.util.Collection;

@Data
public class Group {

    private int totalCount;
    private int successCount;
    private int failureCount;

    private transient Collection<Job> jobs;
    private String key;
    private State state = State.none;
    private Status status = Status.none;
    private int retries;
    private String cron;

    public State calculateState() {
        if (successCount + failureCount == totalCount) {
            return State.complete;
        }
        if (successCount == 0 && failureCount == 0) {
            return State.none;
        }
        return State.running;
    }

    public Status calculateStatus() {
        if (failureCount > 0) {
            return Status.failure;
        }
        if (calculateState() == State.complete) {
            return Status.success;
        }
        return Status.none;
    }

    public String toJSONString() {
        return JSON.toJSONString(this);
    }

    public void updateByJobStatus(Status status) {
        switch (status) {
            case success:
                ++successCount;
                break;
            case failure:
                ++failureCount;
            break;
        }
        setState(calculateState());
        setStatus(calculateStatus());
    }

    public boolean isCompleted() {
        return false;
    }
}
