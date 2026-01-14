#include "tiforth/engine.h"

#include <utility>

namespace tiforth {

arrow::Result<std::unique_ptr<Engine>> Engine::Create(EngineOptions options) {
  return std::unique_ptr<Engine>(new Engine(std::move(options)));
}

Engine::Engine(EngineOptions options) : options_(std::move(options)) {}

Engine::~Engine() = default;

}  // namespace tiforth
