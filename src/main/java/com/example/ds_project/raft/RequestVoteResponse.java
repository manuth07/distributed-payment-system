package com.example.ds_project.raft;

public record RequestVoteResponse(long term, boolean voteGranted) {
}
