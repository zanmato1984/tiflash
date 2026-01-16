#include "tiforth/engine.h"

#include <utility>

#include <arrow/status.h>

#include "tiforth/function_registry.h"
#include "tiforth/spill.h"

namespace tiforth {

arrow::Result<std::unique_ptr<Engine>> Engine::Create(EngineOptions options) {
  if (options.memory_pool == nullptr) {
    return arrow::Status::Invalid("EngineOptions.memory_pool must not be null");
  }
  if (options.spill_manager == nullptr) {
    options.spill_manager = std::make_shared<DenySpillManager>();
  }
  if (options.function_registry == nullptr) {
    options.function_registry = FunctionRegistry::MakeDefault();
  }
  return std::unique_ptr<Engine>(new Engine(std::move(options)));
}

Engine::Engine(EngineOptions options)
    : options_(std::move(options)),
      memory_pool_(options_.memory_pool),
      spill_manager_(options_.spill_manager),
      function_registry_(options_.function_registry) {}

Engine::~Engine() = default;

}  // namespace tiforth
