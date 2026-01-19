// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Core/Block.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/TiForth/ArrowTypeMapping.h>

#include <arrow/memory_pool.h>

#include <memory>
#include <unordered_map>

namespace tiforth
{
class Engine;
class Pipeline;
class Task;
} // namespace tiforth

namespace DB::TiForth
{

class TiForthPipelineBlockInputStream final : public IProfilingBlockInputStream
{
public:
    TiForthPipelineBlockInputStream(
        const String & name_,
        const BlockInputStreamPtr & input_stream_,
        std::unique_ptr<tiforth::Engine> engine_,
        std::unique_ptr<tiforth::Pipeline> pipeline_,
        const NamesAndTypesList & output_columns_,
        const std::unordered_map<String, ColumnOptions> & input_options_by_name_,
        std::shared_ptr<arrow::MemoryPool> pool_holder_,
        const Block & sample_input_block_);

    ~TiForthPipelineBlockInputStream() override;

    String getName() const override { return name; }

    Block getHeader() const override;

protected:
    void readPrefixImpl() override;

    Block readImpl() override;

    void readSuffixImpl() override;

private:
    void initOnce();

    String name;
    BlockInputStreamPtr input_stream;
    std::unique_ptr<tiforth::Engine> engine;
    std::unique_ptr<tiforth::Pipeline> pipeline;
    std::unique_ptr<tiforth::Task> task;
    NamesAndTypesList output_columns;
    std::unordered_map<String, ColumnOptions> input_options_by_name;
    std::shared_ptr<arrow::MemoryPool> pool_holder;
    Block sample_input_block;

    bool initialized = false;
    bool prefix_called = false;
    bool suffix_called = false;
    bool input_closed = false;
    bool pushed_any_input = false;
    bool finished = false;
};

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
