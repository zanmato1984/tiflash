#include <cstdlib>

#include <gtest/gtest.h>

#include "tiforth_c/tiforth.h"

namespace {

void FreeStatus(tiforth_status_t* status) {
  if (status == nullptr) {
    return;
  }
  if (status->message != nullptr) {
    tiforth_free(status->message);
    status->message = nullptr;
  }
}

}  // namespace

TEST(TiForthCapiSmokeTest, EngineCreateReturnsNotImplemented) {
  tiforth_engine_options_t options{};
  options.abi_version = TIFORTH_C_ABI_VERSION;
  tiforth_engine_t* engine = nullptr;

  tiforth_status_t status = tiforth_engine_create(&options, &engine);
  EXPECT_EQ(status.code, TIFORTH_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(engine, nullptr);
  FreeStatus(&status);
}

TEST(TiForthCapiSmokeTest, EngineCreateRejectsBadAbiVersion) {
  tiforth_engine_options_t options{};
  options.abi_version = TIFORTH_C_ABI_VERSION + 1;
  tiforth_engine_t* engine = nullptr;

  tiforth_status_t status = tiforth_engine_create(&options, &engine);
  EXPECT_EQ(status.code, TIFORTH_STATUS_INVALID_ARGUMENT);
  EXPECT_EQ(engine, nullptr);
  FreeStatus(&status);
}

