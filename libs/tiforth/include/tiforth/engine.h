#pragma once

#include <memory>

#include <arrow/memory_pool.h>
#include <arrow/result.h>

namespace tiforth {

struct EngineOptions {
  arrow::MemoryPool* memory_pool = arrow::default_memory_pool();
};

class Engine {
 public:
  static arrow::Result<std::unique_ptr<Engine>> Create(EngineOptions options);

  Engine(const Engine&) = delete;
  Engine& operator=(const Engine&) = delete;

  ~Engine();

  arrow::MemoryPool* memory_pool() const { return memory_pool_; }

 private:
  explicit Engine(EngineOptions options);

  [[maybe_unused]] EngineOptions options_;
  arrow::MemoryPool* memory_pool_ = arrow::default_memory_pool();
};

}  // namespace tiforth
