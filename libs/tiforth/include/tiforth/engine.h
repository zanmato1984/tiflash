#pragma once

#include <memory>

#include <arrow/result.h>

namespace tiforth {

struct EngineOptions {};

class Engine {
 public:
  static arrow::Result<std::unique_ptr<Engine>> Create(EngineOptions options);

  Engine(const Engine&) = delete;
  Engine& operator=(const Engine&) = delete;

  ~Engine();

 private:
  explicit Engine(EngineOptions options);

  [[maybe_unused]] EngineOptions options_;
};

}  // namespace tiforth
