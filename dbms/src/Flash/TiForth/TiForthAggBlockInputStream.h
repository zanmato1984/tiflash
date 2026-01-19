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
} // namespace tiforth

namespace DB::TiForth
{

class TiForthAggBlockInputStream final : public IProfilingBlockInputStream
{
public:
    TiForthAggBlockInputStream(
        const BlockInputStreamPtr & input_stream_,
        std::unique_ptr<tiforth::Engine> engine_,
        std::unique_ptr<tiforth::Pipeline> pipeline_,
        const NamesAndTypesList & output_columns_,
        const std::unordered_map<String, ColumnOptions> & input_options_by_name_,
        arrow::MemoryPool * pool_,
        const Block & sample_input_block_);

    String getName() const override { return "TiForthAgg"; }

    Block getHeader() const override;

protected:
    void readPrefixImpl() override;

    Block readImpl() override;

    void readSuffixImpl() override;

private:
    void initOnce();

    BlockInputStreamPtr input_stream;
    std::unique_ptr<tiforth::Engine> engine;
    std::unique_ptr<tiforth::Pipeline> pipeline;
    NamesAndTypesList output_columns;
    std::unordered_map<String, ColumnOptions> input_options_by_name;
    arrow::MemoryPool * pool = nullptr;
    Block sample_input_block;

    BlocksList output_blocks;
    BlocksList::iterator output_it;
    bool initialized = false;
    bool prefix_called = false;
    bool suffix_called = false;
};

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)

