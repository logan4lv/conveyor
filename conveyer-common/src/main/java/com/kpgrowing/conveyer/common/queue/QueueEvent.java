package com.kpgrowing.conveyer.common.queue;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Builder
@Data
public class QueueEvent {
    public enum Type {
        GROUP_ADD,
        GROUP_COMPLETE,
        JOB_STOP;
    }

    private Type type;
    private List<Group> groups;
    private List<Job> jobs;
}
