#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/compute/exec.h>
#include <arrow/datum.h>
#include <arrow/result.h>

#include "tiforth/type_metadata.h"

namespace arrow {
class Field;
}  // namespace arrow

namespace tiforth {

struct TypedDatum {
  arrow::Datum datum;
  std::shared_ptr<arrow::Field> field;
  LogicalType logical_type;
};

class Function {
 public:
  virtual ~Function() = default;

  virtual bool CanExecute(const std::vector<TypedDatum>& args) const = 0;

  virtual arrow::Result<TypedDatum> Execute(const std::vector<TypedDatum>& args,
                                            arrow::compute::ExecContext* exec_context) const = 0;
};

class FunctionRegistry {
 public:
  static std::shared_ptr<FunctionRegistry> MakeDefault();

  void Register(std::string name, std::shared_ptr<Function> function);

  arrow::Result<TypedDatum> Call(const std::string& name, const std::vector<TypedDatum>& args,
                                 arrow::compute::ExecContext* exec_context) const;

 private:
  struct Entry {
    std::vector<std::shared_ptr<Function>> overloads;
  };

  std::unordered_map<std::string, Entry> functions_;
};

}  // namespace tiforth

