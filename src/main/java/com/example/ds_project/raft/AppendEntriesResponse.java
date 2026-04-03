package com.example.ds_project.raft;

public record AppendEntriesResponse(long term, boolean success, long matchIndex, long conflictIndex) {
}
