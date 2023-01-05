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

#include <Flash/Executor/ResultHandler.h>
#include <Operators/OperatorPipeline.h>

#include <deque>
#include <vector>

namespace tipb
{
class DAGRequest;
}

namespace DB
{
class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;

class Event;
using EventPtr = std::shared_ptr<Event>;
using Events = std::vector<EventPtr>;

class PhysicalPlanNode;
using PhysicalPlanNodePtr = std::shared_ptr<PhysicalPlanNode>;

class PipelineExecStatus;

class Pipeline : public std::enable_shared_from_this<Pipeline>
{
public:
    void addPlan(const PhysicalPlanNodePtr & plan);

    void addDependency(const PipelinePtr & dependency);

    void toTreeString(FmtBuffer & buffer, size_t level = 0) const;

    // only used for test.
    void addGetResultSink(ResultHandler result_handler);

    OperatorPipelineGroups transform(Context & context, size_t concurrency);

    Events toEvents(PipelineExecStatus & status, Context & context, size_t concurrency);

    static bool isSupported(const tipb::DAGRequest & dag_request);

private:
    void toSelfString(FmtBuffer & buffer, size_t level) const;

    EventPtr toEvent(PipelineExecStatus & status, Context & context, size_t concurrency, Events & all_events);

private:
    // data flow: plans.begin() <-- plans.end()
    std::deque<PhysicalPlanNodePtr> plans;

    std::vector<std::weak_ptr<Pipeline>> dependencies;
};

class PipelineBuilder
{
public:
    PipelinePtr addPipeline()
    {
        pipelines.push_back(std::make_shared<Pipeline>());
        return pipelines.back();
    }

    Pipelines build()
    {
        return std::move(pipelines);
    }

private:
    Pipelines pipelines;
};
} // namespace DB
