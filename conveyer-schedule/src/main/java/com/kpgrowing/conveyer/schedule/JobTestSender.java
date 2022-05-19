package com.kpgrowing.conveyer.schedule;

import com.kpgrowing.conveyer.common.queue.Group;
import com.kpgrowing.conveyer.common.queue.Job;
import com.kpgrowing.conveyer.common.support.LoganThread;

import java.util.ArrayList;
import java.util.Random;

public class JobTestSender extends LoganThread {

    private static final String GROUP_KEY_PREFIX = "dataex-resource-";
    private static final String JOB_KEY_PREFIX = "dataex-auth-";
    private final Dispatcher dispatcher;

    public JobTestSender(Dispatcher dispatcher) {
        super("job-test-sender");
        this.dispatcher = dispatcher;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (int i = 0; i < 10; i++) {
                Group group = generateGroup();
                dispatcher.offerGroup(group);
            }
        }
    }

    public static Group generateGroup() {
        String GROUP_KEY = GROUP_KEY_PREFIX + new Random().nextInt(30);

        Group group = new Group();
        group.setJobs(new ArrayList<>());
        group.setKey(GROUP_KEY);
        group.setTotalCount(3);
        group.setCron("0 0/" + (new Random().nextInt(4) + 1) + " * * * *");

        for (int j = 1; j <= 3; j++) {
            Job job = new Job();
            job.setGroupKey(GROUP_KEY);
            job.setPriority(j);
            job.setKey(GROUP_KEY + JOB_KEY_PREFIX + j);
            group.getJobs().add(job);
        }

        return group;
    }
}
