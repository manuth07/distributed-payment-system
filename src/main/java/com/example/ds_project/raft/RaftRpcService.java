package com.example.ds_project.raft;

import com.example.ds_project.coordination.LeaderState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.concurrent.locks.ReentrantLock;

@Service
public class RaftRpcService {
    private static final Logger log = LoggerFactory.getLogger(RaftRpcService.class);
    
    private final RaftNode raftNode;
    private final RaftLog raftLog;
    private final LeaderState leaderState;
    private final RaftLeaderManager raftLeaderManager;

    public RaftRpcService(RaftNode raftNode, RaftLog raftLog, LeaderState leaderState, @Lazy RaftLeaderManager raftLeaderManager) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.leaderState = leaderState;
        this.raftLeaderManager = raftLeaderManager;
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        ReentrantLock lock = raftNode.getLock();
        lock.lock();
        try {
            long currentTerm = raftNode.getCurrentTerm();
            
            // Raft paper §5.1: If RPC request or response contains term T > currentTerm:
            // set currentTerm = T, convert to follower
            if (request.term() > currentTerm) {
                raftNode.setCurrentTerm(request.term());
                raftNode.setVotedFor(null);
                raftNode.setState(RaftNode.State.FOLLOWER);
            }

            // 1. Reply false if term < currentTerm (§5.1)
            if (request.term() < raftNode.getCurrentTerm()) {
                return new RequestVoteResponse(raftNode.getCurrentTerm(), false);
            }

            // Pre-condition logic from prompt: Coordinate with ZooKeeper.
            // "if ZK already elected a leader for this term, do not start a new election - check /payment-leader znode first"
            if (leaderState.isLeader()) {
                // If we are currently the ZK leader, we should ideally lead the Raft cluster as well,
                // but for RequestVote, we evaluate based on standard Raft rules to prevent split brain.
            }

            boolean voteGranted = false;
            String votedFor = raftNode.getVotedFor();

            // 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (§5.2, §5.4)
            if (votedFor == null || votedFor.equals(request.candidateId())) {
                long lastLogIndex = raftLog.getLastLogIndex();
                long lastLogTerm = raftLog.getLastLogTerm();

                boolean logIsUpToDate = (request.lastLogTerm() > lastLogTerm) ||
                                        (request.lastLogTerm() == lastLogTerm && request.lastLogIndex() >= lastLogIndex);

                if (logIsUpToDate) {
                    raftNode.setVotedFor(request.candidateId());
                    voteGranted = true;
                    log.info("Granted vote to {} for term {}", request.candidateId(), request.term());
                    
                    // Reset election timer should happen here inherently via follower timeout loop intercept, 
                    // which we will handle in Step 5 (Leader logic / Election timer).
                } else {
                    log.debug("Rejected vote for {} due to outdated log", request.candidateId());
                }
            } else {
                log.debug("Rejected vote for {} because already voted for {}", request.candidateId(), votedFor);
            }

            return new RequestVoteResponse(raftNode.getCurrentTerm(), voteGranted);

        } finally {
            lock.unlock();
        }
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        ReentrantLock lock = raftNode.getLock();
        lock.lock();
        try {
            long currentTerm = raftNode.getCurrentTerm();

            // 1. Reply false if term < currentTerm (§5.1)
            if (request.term() < currentTerm) {
                return new AppendEntriesResponse(currentTerm, false, 0, 0);
            }

            // Acknowledge valid leader
            if (request.term() > currentTerm) {
                raftNode.setCurrentTerm(request.term());
                raftNode.setVotedFor(null);
            }
            
            // Revert/refresh to FOLLOWER when valid AppendEntries is received
            raftNode.setState(RaftNode.State.FOLLOWER);
            // *** Critical: Reset election timer so we don't start a spurious election ***
            raftLeaderManager.resetElectionTimeout();
            
            long lastIndex = raftLog.getLastLogIndex();

            // 2. Reply false if log doesnt contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
            if (request.prevLogIndex() > lastIndex) {
                return new AppendEntriesResponse(raftNode.getCurrentTerm(), false, 0, lastIndex + 1);
            }

            if (request.prevLogIndex() >= 0) {
                LogEntry prevEntry = raftLog.getEntry(request.prevLogIndex());
                if (prevEntry == null || prevEntry.getTerm() != request.prevLogTerm()) {
                    return new AppendEntriesResponse(raftNode.getCurrentTerm(), false, 0, request.prevLogIndex());
                }
            }

            // 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
            // 4. Append any new entries not already in the log
            long currentIndex = request.prevLogIndex() + 1;
            if (request.entries() != null) {
                for (LogEntry newEntry : request.entries()) {
                    LogEntry existing = raftLog.getEntry(currentIndex);
                    if (existing != null && existing.getTerm() != newEntry.getTerm()) {
                        raftLog.truncateFromIndex(currentIndex);
                        existing = null;
                    }
                    if (existing == null) {
                        raftLog.appendEntry(newEntry);
                    }
                    currentIndex++;
                }
            }

            // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if (request.leaderCommit() > raftNode.getCommitIndex()) {
                long lastNewEntryIndex = request.prevLogIndex() + (request.entries() == null ? 0 : request.entries().size());
                raftNode.setCommitIndex(Math.min(request.leaderCommit(), lastNewEntryIndex));
            }

            return new AppendEntriesResponse(raftNode.getCurrentTerm(), true, raftLog.getLastLogIndex(), 0);

        } finally {
            lock.unlock();
        }
    }
}
