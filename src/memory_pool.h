#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

#include <vector>
#include <mutex>
#include <string_view>

struct MemoryBlock {
    char data[1024]; // 1KB per block
    bool in_use;
};

class MemoryPool {
public:
    // Pre-allocates memory chunk
    MemoryPool(size_t numBlocks);
    ~MemoryPool() = default;

    // Returns a pointer to a free MemoryBlock, or nullptr if pool is full
    MemoryBlock* Allocate();

    // Returns a block to the pool (Manual Memory Management)
    void Deallocate(MemoryBlock* block);

private:
    std::vector<MemoryBlock> pool_;
    std::mutex poolMutex_;
};

#endif // MEMORY_POOL_H
