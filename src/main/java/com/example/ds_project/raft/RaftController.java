package com.example.ds_project.raft;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftRpcService raftRpcService;

    public RaftController(RaftRpcService raftRpcService) {
        this.raftRpcService = raftRpcService;
    }

    // Raft paper §5.2: RequestVote RPC endpoint
    @PostMapping("/request-vote")
    public RequestVoteResponse requestVote(@RequestBody RequestVoteRequest request) {
        return raftRpcService.handleRequestVote(request);
    }

    // Raft paper §5.3: AppendEntries RPC endpoint (also serves as Heartbeat)
    @PostMapping("/append-entries")
    public AppendEntriesResponse appendEntries(@RequestBody AppendEntriesRequest request) {
        return raftRpcService.handleAppendEntries(request);
    }
}
