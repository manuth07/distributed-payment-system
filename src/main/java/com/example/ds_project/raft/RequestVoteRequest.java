package com.example.ds_project.raft;

public record RequestVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
}
