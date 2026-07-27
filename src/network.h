#ifndef NETWORK_H
#define NETWORK_H

#include <string>
#include <functional>
#include <atomic>
#include <string_view>
#include "memory_pool.h"

class Network {
public:
    static void Init();
    static void Cleanup();
    
    // Starts an Async TCP server. 
    // Uses the MemoryPool to avoid runtime RAM allocation.
    static void StartServer(int port, 
                            std::function<std::string(std::string_view)> handler, 
                            std::atomic<bool>& running,
                            MemoryPool& memPool);
    
    // Connects to a port, sends a payload, and returns the response.
    static std::string SendPayload(int port, const std::string& payload);
};

#endif // NETWORK_H
