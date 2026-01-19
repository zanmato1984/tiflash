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
#include <vector>

namespace tiforth
{
class Engine;
class Pipeline;
class Task;
} // namespace tiforth

namespace DB::TiForth
{

class TiForthHashJoinBlockInputStream final : public IProfilingBlockInputStream
{
public:
    TiForthHashJoinBlockInputStream(
        const BlockInputStreamPtr & probe_stream_,
        const BlockInputStreamPtr & build_stream_,
        std::vector<String> probe_key_names_,
        std::vector<String> build_key_names_,
        const NamesAndTypesList & output_columns_,
        const std::unordered_map<String, ColumnOptions> & input_options_by_name_,
        std::shared_ptr<arrow::MemoryPool> pool_holder_,
        const Block & sample_probe_block_,
        const Block & sample_build_block_);

    ~TiForthHashJoinBlockInputStream() override;

    String getName() const override { return "TiForthHashJoin"; }

    Block getHeader() const override;

protected:
    void readPrefixImpl() override;

    Block readImpl() override;

    void readSuffixImpl() override;

private:
    void initOnce();

    BlockInputStreamPtr probe_stream;
    BlockInputStreamPtr build_stream;
    std::vector<String> probe_key_names;
    std::vector<String> build_key_names;

    std::unique_ptr<tiforth::Engine> engine;
    std::unique_ptr<tiforth::Pipeline> pipeline;
    std::unique_ptr<tiforth::Task> task;
    NamesAndTypesList output_columns;
    std::unordered_map<String, ColumnOptions> input_options_by_name;
    std::shared_ptr<arrow::MemoryPool> pool_holder;
    Block sample_probe_block;
    Block sample_build_block;

    bool initialized = false;
    bool prefix_called = false;
    bool suffix_called = false;
    bool probe_input_closed = false;
    bool pushed_any_probe_input = false;
    bool finished = false;
};

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
