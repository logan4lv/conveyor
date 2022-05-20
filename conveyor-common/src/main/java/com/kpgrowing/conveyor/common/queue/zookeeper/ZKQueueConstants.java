package com.kpgrowing.conveyor.common.queue.zookeeper;

import static org.apache.curator.utils.ZKPaths.makePath;

public interface ZKQueueConstants {
    public final static String PATH_GROUPS = "/groups";
    public final static String PATH_NOTIFY_ADD = makePath("notify", "group-add");
    public final static String PATH_NOTIFY_COMPLETE = makePath("notify", "group-complete");
    public final static String PATH_NOTIFY_JOB_STOP = makePath("notify", "job-stop");
    public final static String GROUP_SEQUENTIAL_SEPARATOR = "-group-";
    public final static String NODE_LOCK_GROUP = "lock-group";
    public final static String NODE_LOCK_JOB = "lock";
    public final static String PATH_LOCK_RW = "/RW_LOCK";

    public final static String NAMESPACE = "DataEx-Scheduler-Queue";
    public final static String PATH_WAITING_GROUPS = "/waiting-groups";
    public final static String PATH_LOCK_WAITING_GROUPS = "/LOCK_WAITING_GROUPS";
}
