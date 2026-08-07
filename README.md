# Raft Consensus Engine

A fault-tolerant distributed consensus engine written from scratch in modern C++17. This project implements the core mechanics of the **Raft Consensus Protocol**, enabling a distributed cluster of nodes to achieve highly available state replication even in the event of hardware or network failures.

This engine was engineered with a heavy focus on **low-level hardware optimization**, specifically bypassing standard Operating System bottlenecks for maximum throughput.

## 🚀 Architectural Highlights

### Zero-Copy Asynchronous Networking
Standard socket programming blocks the CPU and forces context switches. This engine utilizes **OS-level event multiplexing (`select`)** to build a non-blocking, asynchronous event loop. 
* A single thread can manage all incoming TCP connections concurrently.
* Idle CPU utilization sits at a flat **0.0%** while waiting for network packets.

### Pre-Allocated Memory Pooling
Dynamic memory allocation (`new`, `malloc`, or `std::string`) during runtime triggers the OS memory allocator and garbage collector, introducing unpredictable latency. 
* This engine uses a custom **Memory Pool** that pre-allocates contiguous memory blocks upon boot. 
* Incoming network streams are parsed using lightweight `std::string_view` pointers directly mapped to the pre-allocated network buffers, completely eliminating deep memory copies and heap allocations during RPC routing.

### Strict Concurrency & Deadlock Avoidance
Distributed systems are notoriously prone to race conditions and Mexican-standoff deadlocks. 
* The engine utilizes strict memory barriers (`std::mutex`) and distinct background threads for leader elections, heartbeats, and disk persistence. 
* Memory locks are instantly released *prior* to executing blocking TCP network calls, ensuring the cluster never deadlocks during a staggered election scenario.

## ⚙️ How to Build and Run

### Prerequisites
* Windows OS
* `g++` compiler (MinGW-w64) supporting C++17

### Compilation
```bash
g++ -std=c++17 src/main.cpp src/raft_node.cpp src/network.cpp src/memory_pool.cpp -lws2_32 -o raft_engine.exe
```

### Running the Cluster
You will need to open 3 separate terminal windows to simulate a 3-node distributed cluster.

**Window 1:**
```bash
.\raft_engine.exe 1
```
**Window 2:**
```bash
.\raft_engine.exe 2
```
**Window 3:**
```bash
.\raft_engine.exe 3
```

Watch the terminals as the nodes discover each other, hold a cryptographic election, and designate a Leader. Try killing the Leader (`Ctrl+C`) to watch the followers instantly detect the failure and elect a new Leader to maintain cluster availability!

## 🛠️ Technologies Used
* **Language:** C++17
* **Networking:** Winsock2 (TCP/IP)
* **Concurrency:** `<thread>`, `<mutex>`, `<atomic>`
* **Memory Management:** `<string_view>`, Custom Pool Allocators
