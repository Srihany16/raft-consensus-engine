#ifndef RAFT_NODE_H
#define RAFT_NODE_H

#include <string>
#include <string>
#include <mutex>
#include <thread>
#include <atomic>
#include "memory_pool.h"

enum class NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

struct LogEntry {
    int term;
    std::string command;
};

// RPC Arguments and Replies
struct RequestVoteArgs {
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;
};

struct RequestVoteReply {
    int term;
    bool voteGranted;
};

struct AppendEntriesArgs {
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<LogEntry> entries;
    int leaderCommit;
};

struct AppendEntriesReply {
    int term;
    bool success;
};

class RaftNode {
public:
    RaftNode(int id, const std::vector<int>& peerIds);
    ~RaftNode();

    // Starts the node (launches background threads)
    void Start();
    
    // Stops the node
    void Stop();

    // Client Interface
    bool ReceiveClientCommand(std::string command);

private:
    int id_;
    std::vector<int> peerIds_;
    
    // Persistent state on all servers
    int currentTerm_;
    int votedFor_;
    std::vector<LogEntry> log_;

    // Volatile state on all servers
    int commitIndex_;

    // Volatile state on leaders
    std::vector<int> nextIndex_;
    std::vector<int> matchIndex_;

    int lastApplied_;

    std::atomic<NodeState> state_;
    std::atomic<bool> running_;
    std::mutex mutex_;

    // Advanced Memory Management
    MemoryPool memPool_;

    // Background threads
    std::thread electionThread_;
    std::thread serverThread_;
    std::thread heartbeatThread_;

    // Background Threads
    void RunElectionTimer();
    void RunHeartbeatTimer();
    void StartRpcServer();
    void BecomeCandidate();
    
    // RPC Handlers
    RequestVoteReply HandleRequestVote(const RequestVoteArgs& args);
    AppendEntriesReply HandleAppendEntries(const AppendEntriesArgs& args);

    // Persistence (Phase 4)
    void SaveState();
    void LoadState();
};

#endif // RAFT_NODE_H
