#include "network.h"
#include <iostream>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
// POSIX
#endif

void Network::Init() {
#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed." << std::endl;
    }
#endif
}

void Network::Cleanup() {
#ifdef _WIN32
    WSACleanup();
#endif
}

void Network::StartServer(int port, std::function<std::string(std::string_view)> handler, std::atomic<bool>& running, MemoryPool& memPool) {
#ifdef _WIN32
    SOCKET listenSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listenSocket == INVALID_SOCKET) return;

    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(port);

    if (bind(listenSocket, (SOCKADDR*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        closesocket(listenSocket);
        return;
    }

    listen(listenSocket, SOMAXCONN);
    std::cout << "[Network] Zero-Copy Async Server active on port " << port << std::endl;

    while (running) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(listenSocket, &readfds);

        // select() puts the thread to sleep in the OS Kernel until a packet arrives
        timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 100000; // 100ms wakeup

        int activity = select(0, &readfds, NULL, NULL, &timeout);

        if (activity > 0 && FD_ISSET(listenSocket, &readfds)) {
            SOCKET clientSocket = accept(listenSocket, NULL, NULL);
            if (clientSocket != INVALID_SOCKET) {
                
                // --- ZERO ALLOCATION ---
                // Borrow memory from our pre-allocated pool instead of asking Windows
                MemoryBlock* block = memPool.Allocate();
                
                if (block != nullptr) {
                    int bytesReceived = recv(clientSocket, block->data, sizeof(block->data), 0);
                    if (bytesReceived > 0) {
                        
                        // --- ZERO COPY ---
                        // Create a lightweight "view" of the raw memory. 
                        // We do NOT copy the string data.
                        std::string_view payloadView(block->data, bytesReceived);
                        
                        // Parse it using the Raft logic
                        std::string response = handler(payloadView);
                        send(clientSocket, response.c_str(), response.size(), 0);
                    }
                    // --- MANUAL MEMORY MANAGEMENT ---
                    // Crucial: We must return the block or the server will eventually run out of RAM!
                    memPool.Deallocate(block);
                } else {
                    std::cerr << "[Network] SEVERE ERROR: Memory Pool Exhausted!" << std::endl;
                }
                closesocket(clientSocket);
            }
        }
    }
    closesocket(listenSocket);
#endif
}

std::string Network::SendPayload(int port, const std::string& payload) {
#ifdef _WIN32
    SOCKET connectSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (connectSocket == INVALID_SOCKET) return "";

    sockaddr_in clientService;
    clientService.sin_family = AF_INET;
    InetPton(AF_INET, "127.0.0.1", &clientService.sin_addr.s_addr);
    clientService.sin_port = htons(port);

    if (connect(connectSocket, (SOCKADDR*)&clientService, sizeof(clientService)) == SOCKET_ERROR) {
        closesocket(connectSocket);
        return "";
    }

    send(connectSocket, payload.c_str(), payload.size(), 0);

    char buffer[1024];
    int bytesReceived = recv(connectSocket, buffer, sizeof(buffer) - 1, 0);
    std::string response = "";
    if (bytesReceived > 0) {
        buffer[bytesReceived] = '\0';
        response = std::string(buffer);
    }

    closesocket(connectSocket);
    return response;
#else
    return "";
#endif
}
