package com.example.ds_project.raft;

import java.util.List;

public record AppendEntriesRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm, List<LogEntry> entries, long leaderCommit) {
}
