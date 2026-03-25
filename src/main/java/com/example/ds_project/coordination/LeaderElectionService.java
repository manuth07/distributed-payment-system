package com.example.ds_project.coordination;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

@Service
public class LeaderElectionService {

    private static final String ELECTION_ROOT = "/election";
    private static final String NODE_PREFIX = ELECTION_ROOT + "/node-";

    private final ZookeeperClientService zookeeperClientService;
    private final LeaderState leaderState;
    private final WatcherService watcherService;

    @Value("${node.url}")
    private String nodeUrl;

    private String currentZNode;

    public LeaderElectionService(ZookeeperClientService zookeeperClientService, 
                                 LeaderState leaderState, 
                                 WatcherService watcherService) {
        this.zookeeperClientService = zookeeperClientService;
        this.leaderState = leaderState;
        this.watcherService = watcherService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startElection() {
        try {
            CuratorFramework client = zookeeperClientService.getClient();

            // Ensure root path exists
            if (client.checkExists().forPath(ELECTION_ROOT) == null) {
                try {
                    client.create().creatingParentsIfNeeded().forPath(ELECTION_ROOT);
                } catch (KeeperException.NodeExistsException e) {
                    // Ignore if node was created concurrently by another node
                }
            }

            // Create ephemeral sequential node
            currentZNode = client.create()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(NODE_PREFIX, nodeUrl.getBytes(StandardCharsets.UTF_8));
            
            System.out.println("Created znode: " + currentZNode);

            runElection();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void runElection() {
        try {
            CuratorFramework client = zookeeperClientService.getClient();
            List<String> children = client.getChildren().usingWatcher(watcherService).forPath(ELECTION_ROOT);
            Collections.sort(children);

            String nodeName = currentZNode.substring(currentZNode.lastIndexOf('/') + 1);
            int index = children.indexOf(nodeName);

            if (index == -1) {
                System.err.println("Node not found in children list. Attempting to recreate...");
                startElection();
                return;
            }

            if (index == 0) {
                // I am the leader
                leaderState.setLeader(true);
                leaderState.setLeaderUrl(nodeUrl);
                System.out.println("New leader elected: " + nodeUrl);
            } else {
                // I am a follower.
                String leaderNodeName = children.get(0);
                String leaderPath = ELECTION_ROOT + "/" + leaderNodeName;
                byte[] leaderData = client.getData().forPath(leaderPath);
                
                String newLeaderUrl = new String(leaderData, StandardCharsets.UTF_8);
                leaderState.setLeader(false);
                leaderState.setLeaderUrl(newLeaderUrl);
                
                System.out.println("Updated leader URL to: " + newLeaderUrl);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
