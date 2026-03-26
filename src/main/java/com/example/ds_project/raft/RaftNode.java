package com.example.ds_project.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    public enum State {
        FOLLOWER, CANDIDATE, LEADER
    }

    // Raft paper §5.1: State
    private volatile State state = State.FOLLOWER;

    // Persistent state on all servers (Raft paper §5.1)
    private long currentTerm = 0;
    private String votedFor = null;
    
    private final RaftLog raftLog;

    public RaftNode(RaftLog raftLog) {
        this.raftLog = raftLog;
    }

    // Volatile state on all servers (Raft paper §5.1)
    private volatile long commitIndex = -1;
    private volatile long lastApplied = -1;

    // Use ReentrantLock per Code Quality Rules
    private final ReentrantLock lock = new ReentrantLock();

    @Value("${raft.state.file}")
    private String stateFilePath;

    @Value("${raft.node.id}")
    private String nodeId;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        loadState();
    }

    // Raft paper §5.1: Persist currentTerm and votedFor to disk
    public void saveState() {
        lock.lock();
        try {
            File stateFile = new File(stateFilePath);
            stateFile.getParentFile().mkdirs();
            StateData data = new StateData(currentTerm, votedFor);
            objectMapper.writeValue(stateFile, data);
            log.debug("Persisted state: term={}, votedFor={}", currentTerm, votedFor);
        } catch (IOException e) {
            log.error("Failed to save Raft state to disk", e);
        } finally {
            lock.unlock();
        }
    }

    private void loadState() {
        lock.lock();
        try {
            File stateFile = new File(stateFilePath);
            if (stateFile.exists()) {
                StateData data = objectMapper.readValue(stateFile, StateData.class);
                this.currentTerm = data.currentTerm();
                this.votedFor = data.votedFor();
                log.info("Loaded state from disk: term={}, votedFor={}", currentTerm, votedFor);
            } else {
                log.info("No prior state found in {}. Starting fresh at term 0.", stateFilePath);
            }
        } catch (IOException e) {
            log.error("Failed to read Raft state from disk", e);
        } finally {
            lock.unlock();
        }
    }

    public State getState() { return state; }
    
    public void setState(State newState) {
        lock.lock();
        try {
            if (this.state != newState) {
                // Log every state transition at INFO level
                log.info("Node {} {}→{} term={}", nodeId, this.state, newState, currentTerm);
                this.state = newState;
            }
        } finally {
            lock.unlock();
        }
    }

    public long getCurrentTerm() { return currentTerm; }
    
    public void setCurrentTerm(long term) {
        lock.lock();
        try {
            this.currentTerm = term;
            saveState();
        } finally {
            lock.unlock();
        }
    }

    public String getVotedFor() { return votedFor; }
    
    public void setVotedFor(String candidateId) {
        lock.lock();
        try {
            this.votedFor = candidateId;
            saveState();
        } finally {
            lock.unlock();
        }
    }

    public long getCommitIndex() { return commitIndex; }
    public void setCommitIndex(long commitIndex) { this.commitIndex = commitIndex; }

    public long getLastApplied() { return lastApplied; }
    public void setLastApplied(long lastApplied) { this.lastApplied = lastApplied; }

    public ReentrantLock getLock() { return lock; }

    // Internal DTO for persistence
    private record StateData(long currentTerm, String votedFor) {}
}
