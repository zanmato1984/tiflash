#pragma once

#include <memory>

#include <arrow/compute/type_fwd.h>
#include <arrow/memory_pool.h>
#include <arrow/result.h>

namespace tiforth {

class SpillManager;

struct EngineOptions {
  arrow::MemoryPool* memory_pool = arrow::default_memory_pool();
  std::shared_ptr<SpillManager> spill_manager;
  std::shared_ptr<arrow::compute::FunctionRegistry> function_registry;
};

class Engine {
 public:
  static arrow::Result<std::unique_ptr<Engine>> Create(EngineOptions options);

  Engine(const Engine&) = delete;
  Engine& operator=(const Engine&) = delete;

  ~Engine();

  arrow::MemoryPool* memory_pool() const { return memory_pool_; }
  SpillManager* spill_manager() const { return spill_manager_.get(); }
  arrow::compute::FunctionRegistry* function_registry() const { return function_registry_.get(); }

 private:
  explicit Engine(EngineOptions options);

  [[maybe_unused]] EngineOptions options_;
  arrow::MemoryPool* memory_pool_ = arrow::default_memory_pool();
  std::shared_ptr<SpillManager> spill_manager_;
  std::shared_ptr<arrow::compute::FunctionRegistry> function_registry_;
};

}  // namespace tiforth
