#include "memory_pool.h"

MemoryPool::MemoryPool(size_t numBlocks) : pool_(numBlocks) {
    for (auto& block : pool_) {
        block.in_use = false;
    }
}

MemoryBlock* MemoryPool::Allocate() {
    std::lock_guard<std::mutex> lock(poolMutex_);
    for (auto& block : pool_) {
        if (!block.in_use) {
            block.in_use = true;
            return &block;
        }
    }
    return nullptr; // Out of memory!
}

void MemoryPool::Deallocate(MemoryBlock* block) {
    std::lock_guard<std::mutex> lock(poolMutex_);
    if (block != nullptr) {
        block->in_use = false;
    }
}
