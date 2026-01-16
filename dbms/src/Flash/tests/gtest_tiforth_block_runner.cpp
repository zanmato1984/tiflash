#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Flash/TiForth/BlockPipelineRunner.h>

#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>

#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/filter.h"
#include "tiforth/pipeline.h"

namespace DB::tests {

namespace {

arrow::Status RunFilterOnBlockWithCollation() {
  // Input: s is Nullable(String) with padding BIN collation.
  auto s_nested = ColumnString::create();
  auto s_null = ColumnUInt8::create();
  const auto append_s = [&](std::optional<std::string_view> v) {
    if (v.has_value()) {
      s_nested->insertData(v->data(), v->size());
      s_null->insert(Field(static_cast<UInt64>(0)));
    } else {
      s_nested->insertDefault();
      s_null->insert(Field(static_cast<UInt64>(1)));
    }
  };
  append_s("a");
  append_s("a ");
  append_s("b");
  append_s(std::nullopt);

  auto s_col = ColumnNullable::create(std::move(s_nested), std::move(s_null));
  auto s_type = makeNullable(std::make_shared<DataTypeString>());

  ColumnsWithTypeAndName cols;
  cols.emplace_back(std::move(s_col), s_type, "s");
  Block input(std::move(cols));

  std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  options_by_name.emplace("s", TiForth::ColumnOptions{.collation_id = 46});  // UTF8MB4_BIN (PAD SPACE)

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

  auto predicate = tiforth::MakeCall(
      "equal", {tiforth::MakeFieldRef("s"),
                tiforth::MakeLiteral(std::make_shared<arrow::StringScalar>("a"))});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [predicate]() -> arrow::Result<tiforth::TransformOpPtr> {
        return std::make_unique<tiforth::FilterTransformOp>(predicate);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());

  ARROW_ASSIGN_OR_RAISE(
      auto outputs,
      TiForth::RunTiForthPipelineOnBlocks(*pipeline, {input}, options_by_name,
                                          arrow::default_memory_pool()));

  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output block");
  }

  const auto & out = outputs[0];
  const auto & block = out.block;
  if (block.columns() != 1 || block.rows() != 2) {
    return arrow::Status::Invalid("unexpected output block shape");
  }

  const auto opt_it = out.options_by_name.find("s");
  if (opt_it == out.options_by_name.end() || !opt_it->second.collation_id.has_value() ||
      *opt_it->second.collation_id != 46) {
    return arrow::Status::Invalid("output collation id mismatch");
  }

  const auto & elem = block.getByName("s");
  const auto * nullable = typeid_cast<const ColumnNullable *>(elem.column.get());
  if (nullable == nullptr) {
    return arrow::Status::Invalid("expected nullable string column");
  }
  const auto & nested = nullable->getNestedColumn();
  const auto * str = typeid_cast<const ColumnString *>(&nested);
  if (str == nullptr) {
    return arrow::Status::Invalid("expected ColumnString nested column");
  }

  if (nullable->isNullAt(0) || nullable->isNullAt(1)) {
    return arrow::Status::Invalid("unexpected nulls in output");
  }
  const auto v0 = str->getDataAt(0);
  const auto v1 = str->getDataAt(1);
  if (std::string_view(v0.data, v0.size) != "a" || std::string_view(v1.data, v1.size) != "a ") {
    return arrow::Status::Invalid("unexpected filtered values");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthBlockRunnerTest, FilterOnBlockWithCollation) {
  auto status = RunFilterOnBlockWithCollation();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace DB::tests

#else

TEST(TiForthBlockRunnerTest, Disabled) {
  GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif

