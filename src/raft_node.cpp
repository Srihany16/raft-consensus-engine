#include "raft_node.h"
#include "network.h"
#include <iostream>

RaftNode::RaftNode(int id, const std::vector<int>& peerIds)
    : id_(id), peerIds_(peerIds), currentTerm_(0), votedFor_(-1),
      commitIndex_(0), lastApplied_(0), state_(NodeState::FOLLOWER), running_(false),
      memPool_(1000) { // Pre-allocate 1000 Memory Blocks for Zero-Copy Networking
    LoadState(); // Restore from disk if we crashed and restarted!
}

RaftNode::~RaftNode() {
    Stop();
}

void RaftNode::Start() {
    running_ = true;
    std::cout << "Node " << id_ << " started as FOLLOWER." << std::endl;
    
    // Spawn background threads
    electionThread_ = std::thread(&RaftNode::RunElectionTimer, this);
    serverThread_ = std::thread(&RaftNode::StartRpcServer, this);
    heartbeatThread_ = std::thread(&RaftNode::RunHeartbeatTimer, this);
}

void RaftNode::Stop() {
    running_ = false;
    if (electionThread_.joinable()) electionThread_.join();
    if (serverThread_.joinable()) serverThread_.join();
    if (heartbeatThread_.joinable()) heartbeatThread_.join();
    std::cout << "Node " << id_ << " stopped." << std::endl;
}

#include <random>
#include <chrono>
#include <fstream>

void RaftNode::RunElectionTimer() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(1500, 3000); // 1.5s to 3.0s for easier terminal viewing

    while (running_) {
        int sleepTime = dist(gen);
        
        // Sleep in smaller chunks so we can exit cleanly when Stop() is called
        for (int i = 0; i < sleepTime && running_; i += 100) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!running_) break;

        if (!running_) break;

        bool startElection = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (state_ == NodeState::FOLLOWER || state_ == NodeState::CANDIDATE) {
                startElection = true;
            }
        } // WE MUST RELEASE THE LOCK HERE!

        if (startElection) {
            std::cout << "\n[Node " << id_ << "] Election timeout! Starting election..." << std::endl;
            BecomeCandidate();
        }
    }
}

void RaftNode::RunHeartbeatTimer() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // Leaders send heartbeat every 500ms
        
        bool isLeader = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            isLeader = (state_ == NodeState::LEADER);
        }

        if (isLeader) {
            for (int peerId : peerIds_) {
                Network::SendPayload(8000 + peerId, "HEARTBEAT");
            }
        }
    }
}

void RaftNode::BecomeCandidate() {
    int currentTermSnapshot = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = NodeState::CANDIDATE;
        currentTerm_++;
        votedFor_ = id_;
        currentTermSnapshot = currentTerm_;
    } // RELEASE THE LOCK BEFORE NETWORKING!

    std::cout << "[Node " << id_ << "] Became CANDIDATE for term " << currentTermSnapshot << std::endl;
    
    // Broadcast RequestVote RPCs to all peers over TCP
    int votesReceived = 1; // We automatically vote for ourselves
    for (int peerId : peerIds_) {
        int peerPort = 8000 + peerId;
        std::string response = Network::SendPayload(peerPort, "VOTE_REQUEST");
        
        if (response == "VOTE_YES") {
            votesReceived++;
        }
    }

    // Check if we secured a majority
    int majority = (peerIds_.size() + 1) / 2 + 1;
    if (votesReceived >= majority) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ == NodeState::CANDIDATE && currentTerm_ == currentTermSnapshot) {
            state_ = NodeState::LEADER;
            std::cout << "************************************************" << std::endl;
            std::cout << "[Node " << id_ << "] WINS ELECTION! Now LEADER for term " << currentTerm_ << std::endl;
            std::cout << "************************************************" << std::endl;
        }
    }
}

void RaftNode::StartRpcServer() {
    int port = 8000 + id_; // E.g., Node 1 runs on Port 8001
    
    // ZERO COPY: We accept a string_view which points directly to the MemoryPool!
    auto requestHandler = [this](std::string_view payload) -> std::string {
        if (payload == "VOTE_REQUEST") {
            RequestVoteArgs args; 
            args.term = currentTerm_ + 1; // Mocking incoming data
            RequestVoteReply reply = HandleRequestVote(args);
            return reply.voteGranted ? "VOTE_YES" : "VOTE_NO";
        }
        else if (payload == "HEARTBEAT") {
            AppendEntriesArgs args;
            args.term = currentTerm_;
            AppendEntriesReply reply = HandleAppendEntries(args);
            return reply.success ? "ACK" : "NACK";
        }
        return "ERROR";
    };

    // Pass the Memory Pool to the network layer
    Network::StartServer(port, requestHandler, running_, memPool_);
}

RequestVoteReply RaftNode::HandleRequestVote(const RequestVoteArgs& args) {
    std::lock_guard<std::mutex> lock(mutex_);
    RequestVoteReply reply;
    reply.term = currentTerm_;
    reply.voteGranted = false;

    // 1. Reject if candidate's term is older than ours
    if (args.term < currentTerm_) {
        return reply;
    }

    // 2. If candidate has a newer term, we immediately step down to FOLLOWER
    if (args.term > currentTerm_) {
        currentTerm_ = args.term;
        state_ = NodeState::FOLLOWER;
        votedFor_ = -1; // Reset our vote for the new term
    }

    // 3. Grant vote if we haven't voted for anyone else this term
    if (votedFor_ == -1 || votedFor_ == args.candidateId) {
        // (In full Raft, we also verify the candidate's log is up-to-date here)
        votedFor_ = args.candidateId;
        reply.voteGranted = true;
        std::cout << "[Node " << id_ << "] Voted for Candidate " << args.candidateId << " in term " << currentTerm_ << std::endl;
    }

    reply.term = currentTerm_;
    return reply;
}

AppendEntriesReply RaftNode::HandleAppendEntries(const AppendEntriesArgs& args) {
    std::lock_guard<std::mutex> lock(mutex_);
    AppendEntriesReply reply;
    reply.term = currentTerm_;
    reply.success = false;

    // 1. Reject if leader's term is older than ours (it's an imposter)
    if (args.term < currentTerm_) {
        return reply;
    }

    // 2. Acknowledge the legitimate leader
    currentTerm_ = args.term;
    state_ = NodeState::FOLLOWER;
    std::cout << "[Node " << id_ << "] Received Heartbeat/Logs from Leader " << args.leaderId << std::endl;
    
    // 3. Log Validation (Phase 3)
    // Check if our log matches the leader's log at prevLogIndex
    if (args.prevLogIndex >= log_.size() || 
       (args.prevLogIndex >= 0 && log_[args.prevLogIndex].term != args.prevLogTerm)) {
        reply.success = false;
        return reply; // Log inconsistency detected, reject it!
    }

    // 4. Append any new entries
    int insertIndex = args.prevLogIndex + 1;
    for (size_t i = 0; i < args.entries.size(); i++) {
        if (insertIndex < log_.size()) {
            log_[insertIndex] = args.entries[i]; // Overwrite conflicting log
        } else {
            log_.push_back(args.entries[i]);     // Add new log
        }
        insertIndex++;
    }

    // 5. Update Commit Index
    if (args.leaderCommit > commitIndex_) {
        commitIndex_ = std::min(args.leaderCommit, (int)log_.size() - 1);
        std::cout << "[Node " << id_ << "] Safely committed logs up to index " << commitIndex_ << std::endl;
    }

    reply.success = true;
    return reply;
}

bool RaftNode::ReceiveClientCommand(std::string command) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Only the Leader can accept commands
    if (state_ != NodeState::LEADER) {
        std::cout << "[Node " << id_ << "] Rejected command '" << command << "' (I am not the leader)." << std::endl;
        return false;
    }

    // Step 1: Append to our own log
    LogEntry newEntry;
    newEntry.term = currentTerm_;
    newEntry.command = command;
    log_.push_back(newEntry);

    std::cout << "[Node " << id_ << "] Leader accepted client command: " << command 
              << " (Log Index: " << log_.size() - 1 << ")" << std::endl;
    
    // Step 2: In a full network implementation, we broadcast this to all followers here
    SaveState(); // Save to disk immediately!
    return true;
}

void RaftNode::SaveState() {
    std::string filename = "raft_state_" + std::to_string(id_) + ".dat";
    std::ofstream outFile(filename, std::ios::binary | std::ios::trunc);
    if (outFile.is_open()) {
        outFile.write(reinterpret_cast<char*>(&currentTerm_), sizeof(currentTerm_));
        outFile.write(reinterpret_cast<char*>(&votedFor_), sizeof(votedFor_));
        
        size_t logSize = log_.size();
        outFile.write(reinterpret_cast<char*>(&logSize), sizeof(logSize));
        for (const auto& entry : log_) {
            outFile.write(reinterpret_cast<const char*>(&entry.term), sizeof(entry.term));
            size_t cmdLen = entry.command.size();
            outFile.write(reinterpret_cast<char*>(&cmdLen), sizeof(cmdLen));
            outFile.write(entry.command.c_str(), cmdLen);
        }
        outFile.close();
    }
}

void RaftNode::LoadState() {
    std::string filename = "raft_state_" + std::to_string(id_) + ".dat";
    std::ifstream inFile(filename, std::ios::binary);
    if (inFile.is_open()) {
        inFile.read(reinterpret_cast<char*>(&currentTerm_), sizeof(currentTerm_));
        inFile.read(reinterpret_cast<char*>(&votedFor_), sizeof(votedFor_));
        
        size_t logSize = 0;
        inFile.read(reinterpret_cast<char*>(&logSize), sizeof(logSize));
        log_.clear();
        for (size_t i = 0; i < logSize; i++) {
            LogEntry entry;
            inFile.read(reinterpret_cast<char*>(&entry.term), sizeof(entry.term));
            size_t cmdLen = 0;
            inFile.read(reinterpret_cast<char*>(&cmdLen), sizeof(cmdLen));
            entry.command.resize(cmdLen);
            inFile.read(&entry.command[0], cmdLen);
            log_.push_back(entry);
        }
        inFile.close();
        std::cout << "[Node " << id_ << "] Restored state from disk (Term: " << currentTerm_ << ", Logs: " << log_.size() << ")" << std::endl;
    }
}
