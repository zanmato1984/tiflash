#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/pipeline.h"

namespace tiforth {

namespace {

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBatch(
    const std::shared_ptr<arrow::Schema>& schema, const std::vector<int32_t>& values) {
  arrow::Int32Builder builder;
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(values.size()), {array});
}

arrow::Status RunPassThroughReaderSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));
  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());

  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch(schema, {1, 2}));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeBatch(schema, {3, 4, 5}));

  ARROW_ASSIGN_OR_RAISE(auto input_reader,
                        arrow::RecordBatchReader::Make({batch0, batch1}, schema));
  ARROW_ASSIGN_OR_RAISE(auto output_reader, pipeline->MakeReader(input_reader));

  if (!output_reader->schema()->Equals(*schema, /*check_metadata=*/true)) {
    return arrow::Status::Invalid("unexpected output schema");
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batches;
  while (true) {
    std::shared_ptr<arrow::RecordBatch> out;
    ARROW_RETURN_NOT_OK(output_reader->ReadNext(&out));
    if (out == nullptr) {
      break;
    }
    output_batches.push_back(std::move(out));
  }

  if (output_batches.size() != 2) {
    return arrow::Status::Invalid("expected exactly 2 output batches");
  }
  if (output_batches[0].get() != batch0.get() || output_batches[1].get() != batch1.get()) {
    return arrow::Status::Invalid("expected pass-through output batches");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthPassThroughReaderTest, RecordBatchReader) {
  auto status = RunPassThroughReaderSmoke();
  ASSERT_OK(status);
}

}  // namespace tiforth
