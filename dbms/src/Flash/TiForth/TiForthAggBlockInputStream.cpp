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

#include <Flash/TiForth/TiForthAggBlockInputStream.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Common/Exception.h>
#include <Flash/TiForth/BlockPipelineRunner.h>

#include <arrow/status.h>

#include "tiforth/engine.h"
#include "tiforth/pipeline.h"

namespace DB::TiForth
{

namespace
{

Block makeHeader(const NamesAndTypesList & columns)
{
    Block header;
    for (const auto & col : columns)
    {
        if (col.type == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TiForthAggBlockInputStream output column type must not be null");
        header.insert({col.type->createColumn(), col.type, col.name});
    }
    return header;
}

} // namespace

TiForthAggBlockInputStream::TiForthAggBlockInputStream(
    const BlockInputStreamPtr & input_stream_,
    std::unique_ptr<tiforth::Engine> engine_,
    std::unique_ptr<tiforth::Pipeline> pipeline_,
    const NamesAndTypesList & output_columns_,
    const std::unordered_map<String, ColumnOptions> & input_options_by_name_,
    arrow::MemoryPool * pool_,
    const Block & sample_input_block_)
    : input_stream(input_stream_)
    , engine(std::move(engine_))
    , pipeline(std::move(pipeline_))
    , output_columns(output_columns_)
    , input_options_by_name(input_options_by_name_)
    , pool(pool_)
    , sample_input_block(sample_input_block_)
{
    RUNTIME_CHECK_MSG(input_stream != nullptr, "input stream must not be null");
    RUNTIME_CHECK_MSG(engine != nullptr, "tiforth engine must not be null");
    RUNTIME_CHECK_MSG(pipeline != nullptr, "tiforth pipeline must not be null");
    RUNTIME_CHECK_MSG(pool != nullptr, "arrow memory pool must not be null");
    children.push_back(input_stream);
    output_it = output_blocks.begin();
}

Block TiForthAggBlockInputStream::getHeader() const
{
    return makeHeader(output_columns);
}

void TiForthAggBlockInputStream::readPrefixImpl()
{
    if (prefix_called)
        return;
    prefix_called = true;
    input_stream->readPrefix();
}

void TiForthAggBlockInputStream::readSuffixImpl()
{
    if (suffix_called)
        return;
    suffix_called = true;
    input_stream->readSuffix();
}

void TiForthAggBlockInputStream::initOnce()
{
    if (initialized)
        return;
    initialized = true;

    readPrefixImpl();

    std::vector<Block> input_blocks;
    while (Block block = input_stream->read())
        input_blocks.push_back(std::move(block));

    readSuffixImpl();

    auto outputs_res = RunTiForthPipelineOnBlocks(
        *pipeline,
        input_blocks,
        input_options_by_name,
        pool,
        /*sample_block=*/&sample_input_block);
    if (!outputs_res.ok())
        throw Exception(outputs_res.status().ToString(), ErrorCodes::LOGICAL_ERROR);

    const auto header = getHeader();
    for (auto & out : outputs_res.ValueOrDie())
    {
        Block reordered;
        for (const auto & elem : header)
        {
            if (!out.block.has(elem.name))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "TiForthAggBlockInputStream output is missing required column: {}",
                    elem.name);
            reordered.insert(out.block.getByName(elem.name));
        }
        output_blocks.push_back(std::move(reordered));
    }
    output_it = output_blocks.begin();
}

Block TiForthAggBlockInputStream::readImpl()
{
    initOnce();
    if (output_it == output_blocks.end())
        return Block();

    Block out = *output_it;
    ++output_it;
    return out;
}

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)
