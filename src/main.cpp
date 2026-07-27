#include "raft_node.h"
#include "network.h"
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: raft.exe <node_id>" << std::endl;
        std::cerr << "Example: raft.exe 1" << std::endl;
        return 1;
    }

    int id = std::stoi(argv[1]);
    
    // We assume a 3-node cluster: IDs 1, 2, and 3
    std::vector<int> peers;
    for (int i = 1; i <= 3; i++) {
        if (i != id) peers.push_back(i);
    }

    Network::Init();

    RaftNode node(id, peers);
    node.Start();

    std::cout << "------------------------------------------------" << std::endl;
    std::cout << "Node " << id << " is running!" << std::endl;
    std::cout << "Type a message to save to the database, or 'quit'." << std::endl;
    std::cout << "------------------------------------------------" << std::endl;

    std::string line;
    while (std::getline(std::cin, line)) {
        if (line == "quit" || line == "exit") break;
        if (!line.empty()) {
            node.ReceiveClientCommand(line);
        }
    }

    node.Stop();
    Network::Cleanup();

    return 0;
}
