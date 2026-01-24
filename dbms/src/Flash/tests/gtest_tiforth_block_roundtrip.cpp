#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Common/MyTime.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/TiForth/ArrowBlockConversion.h>
#include <Flash/TiForth/ArrowTypeMapping.h>

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <optional>
#include <string_view>
#include <unordered_map>

namespace DB::tests {

namespace {

arrow::Status RunBlockRoundtrip() {
  // Build a small Block covering the MS8 mapping surface.
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
  append_s(std::nullopt);
  auto s_col = ColumnNullable::create(std::move(s_nested), std::move(s_null));
  auto s_type = makeNullable(std::make_shared<DataTypeString>());

  auto i_type = std::make_shared<DataTypeInt32>();
  auto i_col = ColumnInt32::create();
  i_col->insert(Field(static_cast<Int64>(1)));
  i_col->insert(Field(static_cast<Int64>(2)));
  i_col->insert(Field(static_cast<Int64>(3)));

  auto d_type = createDecimal(/*prec=*/40, /*scale=*/2);
  auto d_mut = d_type->createColumn();
  auto * d_col = typeid_cast<ColumnDecimal<Decimal256> *>(d_mut.get());
  if (d_col == nullptr) {
    return arrow::Status::Invalid("expected Decimal256 column");
  }
  d_col->insert(Decimal256(Int256(123)));
  d_col->insert(Decimal256(Int256(-456)));
  d_col->insert(Decimal256(Int256(0)));

  auto t_type = std::make_shared<DataTypeMyDateTime>(6);
  auto t_col = ColumnUInt64::create();
  const UInt64 dt1 = MyDateTime(2024, 1, 1, 0, 0, 0, 0).toPackedUInt();
  const UInt64 dt2 = MyDateTime(2024, 1, 2, 3, 4, 5, 0).toPackedUInt();
  t_col->insert(Field(dt1));
  t_col->insert(Field(dt2));
  t_col->insert(Field(static_cast<UInt64>(0)));

  ColumnsWithTypeAndName cols;
  cols.emplace_back(std::move(s_col), s_type, "s");
  cols.emplace_back(std::move(i_col), i_type, "i");
  cols.emplace_back(std::move(d_mut), d_type, "d");
  cols.emplace_back(std::move(t_col), t_type, "t");
  Block block(std::move(cols));

  std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  options_by_name.emplace("s", TiForth::ColumnOptions{.collation_id = 46});  // UTF8MB4_BIN (PAD SPACE)

  ARROW_ASSIGN_OR_RAISE(auto batch0,
                        TiForth::toArrowRecordBatch(block, options_by_name,
                                                    arrow::default_memory_pool()));

  ARROW_ASSIGN_OR_RAISE(auto roundtrip, TiForth::fromArrowRecordBatch(batch0));
  ARROW_ASSIGN_OR_RAISE(auto batch1,
                        TiForth::toArrowRecordBatch(roundtrip.block, roundtrip.options_by_name,
                                                    arrow::default_memory_pool()));

  if (!batch0->Equals(*batch1, /*check_metadata=*/true)) {
    return arrow::Status::Invalid("Block->Arrow->Block roundtrip mismatch");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthBlockRoundtripTest, BlockArrowBlock) {
  auto status = RunBlockRoundtrip();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace DB::tests

#else

TEST(TiForthBlockRoundtripTest, Disabled) {
  GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
