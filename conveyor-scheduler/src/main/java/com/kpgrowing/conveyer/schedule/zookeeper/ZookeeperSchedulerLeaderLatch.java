package com.kpgrowing.conveyor.schedule.zookeeper;

import com.kpgrowing.conveyor.schedule.leader.SchedulerLeaderLatchListener;
import com.kpgrowing.conveyor.schedule.leader.SchedulerLeaderLatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.RetryForever;

public class ZookeeperSchedulerLeaderLatch implements SchedulerLeaderLatch {
    @Override
    public void startLeaderElection(SchedulerLeaderLatchListener listener) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1")
                .namespace("DataEx-Scheduler-Queue")
                .retryPolicy(new RetryForever(3000))
                .build();
        client.start();
        LeaderLatch leaderLatch = new LeaderLatch(client, "/scheduler-leader-latch");
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                listener.isLeader();
            }

            @Override
            public void notLeader() {
                listener.notLeader();
            }
        });
        leaderLatch.start();
    }
}
