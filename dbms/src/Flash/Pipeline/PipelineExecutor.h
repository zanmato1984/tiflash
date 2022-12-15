// Copyright 2022 PingCAP, Ltd.
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

#include <Flash/Executor/QueryExecutor.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/PipelineExecStatus.h>

namespace DB
{
class Context;

class PipelineExecutor : public QueryExecutor
{
public:
    explicit PipelineExecutor(
        const ProcessListEntryPtr & process_list_entry_,
        Context & context_,
        const Pipelines & pipelines_)
        : QueryExecutor(process_list_entry_)
        , context(context_)
        , pipelines(pipelines_)
    {
        assert(!pipelines.empty());
        root_pipeline = pipelines[0];
    }

    String dump() const override;

    void cancel() override;

    int estimateNewThreadCount() override;

protected:
    ExecutionResult execute(ResultHandler result_handler) override;

private:
    Context & context;

    Pipelines pipelines;
    PipelinePtr root_pipeline;

    PipelineExecStatus status;
};
} // namespace DB
