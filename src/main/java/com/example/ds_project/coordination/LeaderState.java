package com.example.ds_project.coordination;

import org.springframework.stereotype.Component;

@Component
public class LeaderState {

    private volatile boolean isLeader = false;
    private volatile String leaderUrl = null;

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean leader) {
        isLeader = leader;
    }

    public String getLeaderUrl() {
        return leaderUrl;
    }

    public void setLeaderUrl(String url) {
        leaderUrl = url;
    }
}
