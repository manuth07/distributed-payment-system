package com.example.ds_project.coordination;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Service
public class WatcherService implements CuratorWatcher {

    private final LeaderElectionService leaderElectionService;

    @Autowired
    public WatcherService(@Lazy LeaderElectionService leaderElectionService) {
        this.leaderElectionService = leaderElectionService;
    }

    @Override
    public void process(WatchedEvent event) throws Exception {
        System.out.println("ZooKeeper event triggered: " + event.getType() + " on path " + event.getPath());
        leaderElectionService.runElection();
    }
}
