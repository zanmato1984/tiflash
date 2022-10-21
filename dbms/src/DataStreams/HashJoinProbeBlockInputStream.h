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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/MPMCQueue.h>
#include <Common/ThreadManager.h>

namespace DB
{
class ExpressionActions;

/** Executes a certain expression over the block.
  * Basically the same as ExpressionBlockInputStream,
  * but requires that there must be a join probe action in the Expression.
  *
  * The join probe action is different from the general expression
  * and needs to be executed after join hash map building.
  * We should separate it from the ExpressionBlockInputStream.
  */
class HashJoinProbeBlockInputStream : public IProfilingBlockInputStream
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
    static constexpr auto name = "HashJoinProbe";

public:
    HashJoinProbeBlockInputStream(
        const BlockInputStreamPtr & input,
        const ExpressionActionsPtr & join_probe_actions_,
        const String & req_id,
        std::shared_ptr<MPMCQueue<Block>> queue);

    ~HashJoinProbeBlockInputStream() override
    {
        thread_manager->wait();
    }

    String getName() const override { return name; }
    Block getTotals() override;
    Block getHeader() const override;

    static std::shared_ptr<MPMCQueue<Block>> newQueue()
    {
        return std::make_shared<MPMCQueue<Block>>(40);
    }

protected:
    Block readImpl() override;

    void readThread();

private:
    const LoggerPtr log;
    ExpressionActionsPtr join_probe_actions;
    std::shared_ptr<ThreadManager> thread_manager;
    std::shared_ptr<MPMCQueue<Block>> queue;
};

} // namespace DB
