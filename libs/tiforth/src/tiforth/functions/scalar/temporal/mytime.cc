#include "tiforth/functions/scalar/temporal/mytime.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/function.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/registry.h>
#include <arrow/scalar.h>
#include <arrow/status.h>

namespace tiforth::functions {

namespace {

constexpr const char* kMyTimeOptionsTypeName = "tiforth_mytime";

struct MyTimeOptions final : public arrow::compute::FunctionOptions {
  MyTimeOptions();
  MyTimeOptions(LogicalTypeId type_id, int32_t datetime_fsp);

  static constexpr const char* kTypeName = kMyTimeOptionsTypeName;

  LogicalTypeId type_id = LogicalTypeId::kUnknown;
  int32_t datetime_fsp = -1;
};

class MyTimeOptionsType final : public arrow::compute::FunctionOptionsType {
 public:
  const char* type_name() const override { return MyTimeOptions::kTypeName; }

  std::string Stringify(const arrow::compute::FunctionOptions& options) const override {
    const auto& typed = static_cast<const MyTimeOptions&>(options);
    return "MyTimeOptions{type_id=" + std::to_string(static_cast<int>(typed.type_id)) +
           ", datetime_fsp=" + std::to_string(typed.datetime_fsp) + "}";
  }

  bool Compare(const arrow::compute::FunctionOptions& left,
               const arrow::compute::FunctionOptions& right) const override {
    const auto& l = static_cast<const MyTimeOptions&>(left);
    const auto& r = static_cast<const MyTimeOptions&>(right);
    return l.type_id == r.type_id && l.datetime_fsp == r.datetime_fsp;
  }

  std::unique_ptr<arrow::compute::FunctionOptions> Copy(
      const arrow::compute::FunctionOptions& options) const override {
    const auto& typed = static_cast<const MyTimeOptions&>(options);
    return std::make_unique<MyTimeOptions>(typed.type_id, typed.datetime_fsp);
  }
};

const MyTimeOptionsType kMyTimeOptionsType;

MyTimeOptions::MyTimeOptions() : arrow::compute::FunctionOptions(&kMyTimeOptionsType) {}

MyTimeOptions::MyTimeOptions(LogicalTypeId type_id, int32_t datetime_fsp)
    : arrow::compute::FunctionOptions(&kMyTimeOptionsType),
      type_id(type_id),
      datetime_fsp(datetime_fsp) {}

struct MyTimeState final : public arrow::compute::KernelState {
  explicit MyTimeState(MyTimeOptions options) : options(std::move(options)) {}

  MyTimeOptions options;
};

arrow::Result<std::unique_ptr<arrow::compute::KernelState>> InitMyTimeState(
    arrow::compute::KernelContext*, const arrow::compute::KernelInitArgs& args) {
  const auto* typed = dynamic_cast<const MyTimeOptions*>(args.options);
  if (typed == nullptr) {
    return arrow::Status::Invalid("MyTimeOptions required");
  }
  if (typed->type_id != LogicalTypeId::kMyDate && typed->type_id != LogicalTypeId::kMyDateTime) {
    return arrow::Status::Invalid("MyTimeOptions.type_id must be mydate or mydatetime");
  }
  return std::make_unique<MyTimeState>(*typed);
}

const MyTimeState* GetMyTimeState(arrow::compute::KernelContext* ctx) {
  return ctx != nullptr ? static_cast<const MyTimeState*>(ctx->state()) : nullptr;
}

arrow::Status GetPackedInput(const arrow::compute::ExecSpan& batch,
                            std::shared_ptr<arrow::Array>* array_out,
                            const arrow::UInt64Scalar** scalar_out) {
  if (batch.num_values() != 1) {
    return arrow::Status::Invalid("expected 1 argument");
  }

  if (array_out != nullptr) {
    array_out->reset();
  }
  if (scalar_out != nullptr) {
    *scalar_out = nullptr;
  }

  if (batch[0].is_array()) {
    if (array_out == nullptr) {
      return arrow::Status::Invalid("internal error: missing array_out");
    }
    *array_out = arrow::MakeArray(batch[0].array.ToArrayData());
    if (*array_out == nullptr) {
      return arrow::Status::Invalid("expected non-null array input");
    }
    return arrow::Status::OK();
  }

  if (batch[0].is_scalar()) {
    if (scalar_out == nullptr) {
      return arrow::Status::Invalid("internal error: missing scalar_out");
    }
    *scalar_out = dynamic_cast<const arrow::UInt64Scalar*>(batch[0].scalar);
    if (*scalar_out == nullptr) {
      return arrow::Status::Invalid("expected UInt64Scalar input");
    }
    return arrow::Status::OK();
  }

  return arrow::Status::Invalid("unsupported datum kind for packed mytime input");
}

constexpr uint64_t kYmdMask = ~((uint64_t{1} << 41) - 1);

uint16_t ExtractYear(uint64_t packed) { return static_cast<uint16_t>((packed >> 46) / 13); }
uint8_t ExtractMonth(uint64_t packed) { return static_cast<uint8_t>((packed >> 46) % 13); }
uint8_t ExtractDayOfMonth(uint64_t packed) { return static_cast<uint8_t>((packed >> 41) & 31); }
int64_t ExtractHour(uint64_t packed) { return static_cast<int64_t>((packed >> 36) & 31); }
int64_t ExtractMinute(uint64_t packed) { return static_cast<int64_t>((packed >> 30) & 63); }
int64_t ExtractSecond(uint64_t packed) { return static_cast<int64_t>((packed >> 24) & 63); }
int64_t ExtractMicroSecond(uint64_t packed) {
  return static_cast<int64_t>(packed & ((uint64_t{1} << 24) - 1));
}

// The following date/weekday/week-number algorithms are ports of TiFlash's
// `Common/MyTime.cpp` (which in turn follows TiDB behavior). Keep these helpers
// TiForth-local to avoid any TiFlash symbol dependencies.

constexpr uint32_t kWeekBehaviorMondayFirst = 1;
constexpr uint32_t kWeekBehaviorYear = 2;
constexpr uint32_t kWeekBehaviorFirstWeekday = 4;

int CalcDayNum(int year, int month, int day) {
  // the implementation is the same as TiDB
  if (year == 0 && month == 0) {
    return 0;
  }
  int delsum = 365 * year + 31 * (month - 1) + day;
  if (month <= 2) {
    year--;
  } else {
    delsum -= (month * 4 + 23) / 10;
  }
  int temp = ((year / 100 + 1) * 3) / 4;
  return delsum + year / 4 - temp;
}

int WeekDay(int year, int month, int day) {
  const int current_abs_day_num = CalcDayNum(year, month, day);
  // 1986-01-05 is sunday
  const int reference_abs_day_num = CalcDayNum(1986, 1, 5);
  int diff = current_abs_day_num - reference_abs_day_num;
  if (diff < 0) {
    diff += (-diff / 7 + 1) * 7;
  }
  diff = diff % 7;
  return diff;
}

uint8_t ExtractDayOfWeek(uint64_t packed) {
  return static_cast<uint8_t>(
      WeekDay(ExtractYear(packed), ExtractMonth(packed), ExtractDayOfMonth(packed)) + 1);
}

uint32_t AdjustWeekMode(uint32_t mode) {
  mode &= 7u;
  if (!(mode & kWeekBehaviorMondayFirst)) {
    mode ^= kWeekBehaviorFirstWeekday;
  }
  return mode;
}

// calcWeekday calculates weekday from daynr, returns 0 for Monday, 1 for Tuesday ...
int CalcWeekday(int day_num, bool sunday_first_day_of_week) {
  // the implementation is the same as TiDB
  day_num += 5;
  if (sunday_first_day_of_week) {
    day_num++;
  }
  return day_num % 7;
}

int CalcDaysInYear(int year) {
  // the implementation is the same as TiDB
  if ((year & 3u) == 0 && (year % 100 != 0 || (year % 400 == 0 && (year != 0)))) {
    return 366;
  }
  return 365;
}

std::pair<int, int> CalcWeek(int year, int month, int day, uint32_t mode) {
  int days = 0;
  int ret_year = year;
  int ret_week = 0;
  int day_num = CalcDayNum(year, month, day);
  int first_day_num = CalcDayNum(year, 1, 1);
  bool monday_first = mode & kWeekBehaviorMondayFirst;
  bool week_year = mode & kWeekBehaviorYear;
  bool first_week_day = mode & kWeekBehaviorFirstWeekday;

  int week_day = CalcWeekday(first_day_num, !monday_first);

  if (month == 1 && day <= 7 - week_day) {
    if (!week_year && ((first_week_day && week_day != 0) || (!first_week_day && week_day >= 4))) {
      ret_week = 0;
      return {ret_year, ret_week};
    }
    week_year = true;
    ret_year--;
    days = CalcDaysInYear(ret_year);
    first_day_num -= days;
    week_day = (week_day + 53 * 7 - days) % 7;
  }

  if ((first_week_day && week_day != 0) || (!first_week_day && week_day >= 4)) {
    days = day_num - (first_day_num + 7 - week_day);
  } else {
    days = day_num - (first_day_num - week_day);
  }

  if (week_year && days >= 52 * 7) {
    week_day = (week_day + CalcDaysInYear(ret_year)) % 7;
    if ((!first_week_day && week_day < 4) || (first_week_day && week_day == 0)) {
      ret_year++;
      ret_week = 1;
      return {ret_year, ret_week};
    }
  }
  ret_week = days / 7 + 1;
  return {ret_year, ret_week};
}

uint8_t ExtractWeek(uint64_t packed, uint32_t mode) {
  const auto month = ExtractMonth(packed);
  const auto day = ExtractDayOfMonth(packed);
  if (month == 0 || day == 0) {
    return 0;
  }
  const auto adjusted = AdjustWeekMode(mode);
  const auto [week_year, week] = CalcWeek(ExtractYear(packed), month, day, adjusted);
  (void)week_year;
  return static_cast<uint8_t>(week);
}

uint32_t ExtractYearWeek(uint64_t packed, uint32_t mode) {
  const auto month = ExtractMonth(packed);
  const auto day = ExtractDayOfMonth(packed);
  if (month == 0 || day == 0) {
    return 0;
  }
  const auto adjusted = AdjustWeekMode(mode);
  const auto [week_year, week] = CalcWeek(ExtractYear(packed), month, day, adjusted);
  return static_cast<uint32_t>(week_year * 100 + week);
}

template <typename BuilderT, typename OutputT, typename Fn>
arrow::Status ExecPackedUnary(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                              arrow::compute::ExecResult* out, Fn&& fn) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  const auto* state = GetMyTimeState(ctx);
  if (state == nullptr) {
    return arrow::Status::Invalid("MyTimeState must not be null");
  }
  (void)state;

  std::shared_ptr<arrow::Array> in_array;
  const arrow::UInt64Scalar* in_scalar = nullptr;
  ARROW_RETURN_NOT_OK(GetPackedInput(batch, &in_array, &in_scalar));

  const int64_t rows = batch.length;
  BuilderT builder(ctx->memory_pool());
  ARROW_RETURN_NOT_OK(builder.Reserve(rows));

  const bool scalar_valid = in_scalar != nullptr && in_scalar->is_valid;
  const uint64_t scalar_value = scalar_valid ? in_scalar->value : uint64_t{0};

  const arrow::UInt64Array* u64_arr =
      in_array != nullptr ? static_cast<const arrow::UInt64Array*>(in_array.get()) : nullptr;

  for (int64_t i = 0; i < rows; ++i) {
    const bool is_null = u64_arr != nullptr ? u64_arr->IsNull(i) : !scalar_valid;
    if (is_null) {
      builder.UnsafeAppendNull();
      continue;
    }

    const uint64_t packed = u64_arr != nullptr ? u64_arr->Value(i) : scalar_value;
    const OutputT v = static_cast<OutputT>(fn(packed));
    builder.UnsafeAppend(v);
  }

  std::shared_ptr<arrow::Array> out_array;
  ARROW_RETURN_NOT_OK(builder.Finish(&out_array));
  out->value = out_array->data();
  return arrow::Status::OK();
}

template <typename BuilderT, typename OutputT, typename Fn>
arrow::Status ExecPackedUnaryNullable(arrow::compute::KernelContext* ctx,
                                      const arrow::compute::ExecSpan& batch,
                                      arrow::compute::ExecResult* out, Fn&& fn) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  const auto* state = GetMyTimeState(ctx);
  if (state == nullptr) {
    return arrow::Status::Invalid("MyTimeState must not be null");
  }
  (void)state;

  std::shared_ptr<arrow::Array> in_array;
  const arrow::UInt64Scalar* in_scalar = nullptr;
  ARROW_RETURN_NOT_OK(GetPackedInput(batch, &in_array, &in_scalar));

  const int64_t rows = batch.length;
  BuilderT builder(ctx->memory_pool());
  ARROW_RETURN_NOT_OK(builder.Reserve(rows));

  const bool scalar_valid = in_scalar != nullptr && in_scalar->is_valid;
  const uint64_t scalar_value = scalar_valid ? in_scalar->value : uint64_t{0};

  const arrow::UInt64Array* u64_arr =
      in_array != nullptr ? static_cast<const arrow::UInt64Array*>(in_array.get()) : nullptr;

  for (int64_t i = 0; i < rows; ++i) {
    const bool is_null = u64_arr != nullptr ? u64_arr->IsNull(i) : !scalar_valid;
    if (is_null) {
      builder.UnsafeAppendNull();
      continue;
    }

    const uint64_t packed = u64_arr != nullptr ? u64_arr->Value(i) : scalar_value;
    OutputT v{};
    if (!fn(packed, &v)) {
      builder.UnsafeAppendNull();
      continue;
    }
    builder.UnsafeAppend(v);
  }

  std::shared_ptr<arrow::Array> out_array;
  ARROW_RETURN_NOT_OK(builder.Finish(&out_array));
  out->value = out_array->data();
  return arrow::Status::OK();
}

arrow::Status ExecToYear(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                         arrow::compute::ExecResult* out) {
  return ExecPackedUnary<arrow::UInt16Builder, uint16_t>(ctx, batch, out, ExtractYear);
}

arrow::Status ExecToMonth(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                          arrow::compute::ExecResult* out) {
  return ExecPackedUnary<arrow::UInt8Builder, uint8_t>(ctx, batch, out, ExtractMonth);
}

arrow::Status ExecToDayOfMonth(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                               arrow::compute::ExecResult* out) {
  return ExecPackedUnary<arrow::UInt8Builder, uint8_t>(ctx, batch, out, ExtractDayOfMonth);
}

arrow::Status ExecToMyDate(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                           arrow::compute::ExecResult* out) {
  const auto mask_fn = [](uint64_t packed) -> uint64_t { return packed & kYmdMask; };
  return ExecPackedUnary<arrow::UInt64Builder, uint64_t>(ctx, batch, out, mask_fn);
}

arrow::Status ExecToDayOfWeek(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                              arrow::compute::ExecResult* out) {
  return ExecPackedUnary<arrow::UInt8Builder, uint8_t>(ctx, batch, out, ExtractDayOfWeek);
}

arrow::Status ExecToWeek(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                         arrow::compute::ExecResult* out) {
  const auto fn = [](uint64_t packed) -> uint8_t { return ExtractWeek(packed, /*mode=*/0); };
  return ExecPackedUnary<arrow::UInt8Builder, uint8_t>(ctx, batch, out, fn);
}

arrow::Status ExecToYearWeek(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                             arrow::compute::ExecResult* out) {
  // TiDB/MySQL YEARWEEK semantics: use mode=2 (week number in range 1-53) by default.
  const auto fn = [](uint64_t packed) -> uint32_t { return ExtractYearWeek(packed, /*mode=*/2); };
  return ExecPackedUnary<arrow::UInt32Builder, uint32_t>(ctx, batch, out, fn);
}

arrow::Status ExecTiDBDayOfWeek(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                arrow::compute::ExecResult* out) {
  const auto fn = [](uint64_t packed, uint16_t* v) -> bool {
    if (ExtractMonth(packed) == 0 || ExtractDayOfMonth(packed) == 0) {
      return false;
    }
    *v = static_cast<uint16_t>(ExtractDayOfWeek(packed));
    return true;
  };
  return ExecPackedUnaryNullable<arrow::UInt16Builder, uint16_t>(ctx, batch, out, fn);
}

arrow::Status ExecTiDBWeekOfYear(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                 arrow::compute::ExecResult* out) {
  const auto fn = [](uint64_t packed, uint16_t* v) -> bool {
    if (ExtractMonth(packed) == 0 || ExtractDayOfMonth(packed) == 0) {
      return false;
    }
    *v = static_cast<uint16_t>(ExtractWeek(packed, /*mode=*/3));
    return true;
  };
  return ExecPackedUnaryNullable<arrow::UInt16Builder, uint16_t>(ctx, batch, out, fn);
}

arrow::Status ExecYearWeek(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                           arrow::compute::ExecResult* out) {
  // YEARWEEK(date) in TiDB/MySQL defaults to mode=0 but forces WEEK_BEHAVIOR_YEAR (=> 2).
  const auto fn = [](uint64_t packed, uint32_t* v) -> bool {
    if (ExtractMonth(packed) == 0 || ExtractDayOfMonth(packed) == 0) {
      return false;
    }
    *v = ExtractYearWeek(packed, /*mode=*/2);
    return true;
  };
  return ExecPackedUnaryNullable<arrow::UInt32Builder, uint32_t>(ctx, batch, out, fn);
}

arrow::Status ExecHour(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                       arrow::compute::ExecResult* out) {
  return ExecPackedUnary<arrow::Int64Builder, int64_t>(ctx, batch, out, ExtractHour);
}

arrow::Status ExecMinute(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                         arrow::compute::ExecResult* out) {
  return ExecPackedUnary<arrow::Int64Builder, int64_t>(ctx, batch, out, ExtractMinute);
}

arrow::Status ExecSecond(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                         arrow::compute::ExecResult* out) {
  return ExecPackedUnary<arrow::Int64Builder, int64_t>(ctx, batch, out, ExtractSecond);
}

arrow::Status ExecMicroSecond(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                              arrow::compute::ExecResult* out) {
  return ExecPackedUnary<arrow::Int64Builder, int64_t>(ctx, batch, out, ExtractMicroSecond);
}

class PackedMyTimeMetaFunction final : public arrow::compute::MetaFunction {
 public:
  PackedMyTimeMetaFunction(std::string name, std::string packed_name,
                           arrow::compute::FunctionRegistry* fallback_registry)
      : arrow::compute::MetaFunction(std::move(name), arrow::compute::Arity::Unary(),
                                     arrow::compute::FunctionDoc::Empty()),
        packed_name_(std::move(packed_name)),
        fallback_registry_(fallback_registry) {}

 protected:
  arrow::Result<arrow::Datum> ExecuteImpl(const std::vector<arrow::Datum>& args,
                                         const arrow::compute::FunctionOptions* options,
                                         arrow::compute::ExecContext* ctx) const override {
    if (args.size() != 1) {
      return arrow::Status::Invalid(name(), " requires 1 arg");
    }
    if (fallback_registry_ == nullptr) {
      return arrow::Status::Invalid("fallback function registry must not be null");
    }

    const bool want_packed =
        args[0].type() != nullptr && args[0].type()->id() == arrow::Type::UINT64 &&
        dynamic_cast<const MyTimeOptions*>(options) != nullptr;
    if (want_packed) {
      return arrow::compute::CallFunction(packed_name_, args, options, ctx);
    }

    arrow::compute::ExecContext fallback_ctx(
        ctx != nullptr ? ctx->memory_pool() : arrow::default_memory_pool(),
        ctx != nullptr ? ctx->executor() : nullptr, fallback_registry_);
    return arrow::compute::CallFunction(name(), args, /*options=*/nullptr, &fallback_ctx);
  }

 private:
  std::string packed_name_;
  arrow::compute::FunctionRegistry* fallback_registry_ = nullptr;
};

arrow::compute::ScalarKernel MakePackedKernel(std::shared_ptr<arrow::DataType> out_type,
                                              arrow::compute::ArrayKernelExec exec) {
  arrow::compute::ScalarKernel kernel({arrow::compute::InputType(arrow::Type::UINT64)},
                                      arrow::compute::OutputType(std::move(out_type)), exec,
                                      InitMyTimeState);
  kernel.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
  kernel.can_write_into_slices = false;
  return kernel;
}

}  // namespace

std::unique_ptr<arrow::compute::FunctionOptions> MakeMyTimeOptions(LogicalTypeId type_id,
                                                                   int32_t datetime_fsp) {
  return std::make_unique<MyTimeOptions>(type_id, datetime_fsp);
}

arrow::Status RegisterScalarTemporalFunctions(arrow::compute::FunctionRegistry* registry,
                                              arrow::compute::FunctionRegistry* fallback_registry) {
  if (registry == nullptr) {
    return arrow::Status::Invalid("function registry must not be null");
  }
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback function registry must not be null");
  }

  ARROW_RETURN_NOT_OK(
      registry->AddFunctionOptionsType(&kMyTimeOptionsType, /*allow_overwrite=*/true));

  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "toYear", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakePackedKernel(arrow::uint16(), ExecToYear)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }
  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "toMonth", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakePackedKernel(arrow::uint8(), ExecToMonth)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }
  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "toDayOfMonth", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakePackedKernel(arrow::uint8(), ExecToDayOfMonth)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }
  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "toMyDate", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakePackedKernel(arrow::uint64(), ExecToMyDate)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }
  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "toDayOfWeek", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakePackedKernel(arrow::uint8(), ExecToDayOfWeek)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }
  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "toWeek", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakePackedKernel(arrow::uint8(), ExecToWeek)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }
  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "toYearWeek", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakePackedKernel(arrow::uint32(), ExecToYearWeek)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }
  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "tidbDayOfWeek", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakePackedKernel(arrow::uint16(), ExecTiDBDayOfWeek)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }
  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "tidbWeekOfYear", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakePackedKernel(arrow::uint16(), ExecTiDBWeekOfYear)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }
  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "yearWeek", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakePackedKernel(arrow::uint32(), ExecYearWeek)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }

  // TiDB-like extraction names. These exist in Arrow compute for timestamp types,
  // so install MetaFunctions to only intercept packed-MyTime (UInt64) usage.
  {
    auto hour = std::make_shared<arrow::compute::ScalarFunction>(
        "tiforth.mytime_hour", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(hour->AddKernel(MakePackedKernel(arrow::int64(), ExecHour)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(hour), /*allow_overwrite=*/true));
    ARROW_RETURN_NOT_OK(registry->AddFunction(
        std::make_shared<PackedMyTimeMetaFunction>("hour", "tiforth.mytime_hour", fallback_registry),
        /*allow_overwrite=*/true));
  }
  {
    auto minute = std::make_shared<arrow::compute::ScalarFunction>(
        "tiforth.mytime_minute", arrow::compute::Arity::Unary(),
        arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(minute->AddKernel(MakePackedKernel(arrow::int64(), ExecMinute)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(minute), /*allow_overwrite=*/true));
    ARROW_RETURN_NOT_OK(registry->AddFunction(
        std::make_shared<PackedMyTimeMetaFunction>("minute", "tiforth.mytime_minute", fallback_registry),
        /*allow_overwrite=*/true));
  }
  {
    auto second = std::make_shared<arrow::compute::ScalarFunction>(
        "tiforth.mytime_second", arrow::compute::Arity::Unary(),
        arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(second->AddKernel(MakePackedKernel(arrow::int64(), ExecSecond)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(second), /*allow_overwrite=*/true));
    ARROW_RETURN_NOT_OK(registry->AddFunction(
        std::make_shared<PackedMyTimeMetaFunction>("second", "tiforth.mytime_second", fallback_registry),
        /*allow_overwrite=*/true));
  }
  {
    auto micro = std::make_shared<arrow::compute::ScalarFunction>(
        "tiforth.mytime_micro_second", arrow::compute::Arity::Unary(),
        arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(micro->AddKernel(MakePackedKernel(arrow::int64(), ExecMicroSecond)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(micro), /*allow_overwrite=*/true));

    // "microSecond" is a TiDB/TiFlash naming; keep it as-is and intercept packed usage only.
    ARROW_RETURN_NOT_OK(registry->AddFunction(
        std::make_shared<PackedMyTimeMetaFunction>("microSecond", "tiforth.mytime_micro_second",
                                                   fallback_registry),
        /*allow_overwrite=*/true));
  }

  return arrow::Status::OK();
}

}  // namespace tiforth::functions
