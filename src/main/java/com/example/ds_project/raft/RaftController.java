package com.example.ds_project.raft;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftRpcService raftRpcService;
    private final RaftNode raftNode;

    public RaftController(RaftRpcService raftRpcService, RaftNode raftNode) {
        this.raftRpcService = raftRpcService;
        this.raftNode = raftNode;
    }

    // Raft paper §5.2: RequestVote RPC
    @PostMapping("/request-vote")
    public RequestVoteResponse requestVote(@RequestBody RequestVoteRequest request) {
        return raftRpcService.handleRequestVote(request);
    }

    // Raft paper §5.3: AppendEntries RPC (also serves as heartbeat)
    @PostMapping("/append-entries")
    public AppendEntriesResponse appendEntries(@RequestBody AppendEntriesRequest request) {
        return raftRpcService.handleAppendEntries(request);
    }

    // Debug: GET http://localhost:8081/raft/status
    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of(
                "state",       raftNode.getState().name(),
                "term",        raftNode.getCurrentTerm(),
                "commitIndex", raftNode.getCommitIndex()
        );
    }
}
