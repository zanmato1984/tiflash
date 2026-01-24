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

#include <arrow/result.h>
#include <arrow/status.h>

#include <atomic>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "tiforth/task/blocked_resumer.h"
#include "tiforth/task/task_context.h"
#include "tiforth/task/task_group.h"
#include "tiforth/task/task_status.h"

namespace DB::TiForth
{

class SimpleResumer final : public tiforth::task::Resumer
{
public:
    void Resume() override { resumed_.store(true, std::memory_order_release); }
    bool IsResumed() const override { return resumed_.load(std::memory_order_acquire); }

private:
    std::atomic_bool resumed_{false};
};

class ResumersAwaiter final : public tiforth::task::Awaiter
{
public:
    explicit ResumersAwaiter(tiforth::task::Resumers resumers_)
        : resumers(std::move(resumers_))
    {}

    tiforth::task::Resumers resumers;
};

inline tiforth::task::TaskContext MakeTaskContext()
{
    tiforth::task::TaskContext task_ctx;
    task_ctx.resumer_factory = []() -> arrow::Result<tiforth::task::ResumerPtr> { return std::make_shared<SimpleResumer>(); };
    task_ctx.any_awaiter_factory = [](tiforth::task::Resumers resumers) -> arrow::Result<tiforth::task::AwaiterPtr> {
        return std::make_shared<ResumersAwaiter>(std::move(resumers));
    };
    return task_ctx;
}

enum class AwaiterDriveResult
{
    kProgressed,
    kWaitingExternal,
};

inline arrow::Result<std::optional<tiforth::task::BlockedKind>> DriveBlockedResumer(
    const std::shared_ptr<tiforth::task::BlockedResumer> & resumer)
{
    if (resumer == nullptr)
        return arrow::Status::Invalid("blocked resumer must not be null");

    switch (resumer->kind())
    {
    case tiforth::task::BlockedKind::kIOIn:
    case tiforth::task::BlockedKind::kIOOut:
    {
        return resumer->ExecuteIO();
    }
    case tiforth::task::BlockedKind::kWaiting:
    {
        return resumer->Await();
    }
    case tiforth::task::BlockedKind::kWaitForNotify:
    {
        ARROW_RETURN_NOT_OK(resumer->Notify());
        return std::nullopt;
    }
    }
    return arrow::Status::Invalid("unknown blocked resumer kind");
}

inline arrow::Result<AwaiterDriveResult> DriveAwaiter(const tiforth::task::AwaiterPtr & awaiter)
{
    if (awaiter == nullptr)
        return arrow::Status::Invalid("awaiter must not be null");

    auto resumers_awaiter = std::dynamic_pointer_cast<ResumersAwaiter>(awaiter);
    if (resumers_awaiter == nullptr)
        return arrow::Status::Invalid("TiFlash runner expected ResumersAwaiter");

    bool has_external_pending = false;
    for (const auto & resumer : resumers_awaiter->resumers)
    {
        if (resumer == nullptr)
            return arrow::Status::Invalid("awaiter resumer must not be null");

        if (resumer->IsResumed())
            continue;

        if (auto blocked = std::dynamic_pointer_cast<tiforth::task::BlockedResumer>(resumer); blocked != nullptr)
        {
            ARROW_ASSIGN_OR_RAISE(const auto next, DriveBlockedResumer(blocked));
            if (!next.has_value())
                blocked->Resume();
            continue;
        }

        has_external_pending = true;
    }

    return has_external_pending ? AwaiterDriveResult::kWaitingExternal : AwaiterDriveResult::kProgressed;
}

inline arrow::Status RunTaskToCompletion(const tiforth::task::Task & task, const tiforth::task::TaskContext & task_ctx, tiforth::task::TaskId task_id)
{
    while (true)
    {
        ARROW_ASSIGN_OR_RAISE(auto st, task(task_ctx, task_id));
        if (st.IsFinished())
            return arrow::Status::OK();
        if (st.IsContinue())
            continue;
        if (st.IsYield())
        {
            std::this_thread::yield();
            continue;
        }
        if (st.IsCancelled())
            return arrow::Status::Cancelled("task cancelled");
        if (st.IsBlocked())
        {
            ARROW_ASSIGN_OR_RAISE(const auto drive, DriveAwaiter(st.GetAwaiter()));
            if (drive == AwaiterDriveResult::kWaitingExternal)
                return arrow::Status::Invalid("task blocked on external resumer");
            continue;
        }
        return arrow::Status::Invalid("unknown task status");
    }
}

inline arrow::Status RunTaskGroupToCompletion(const tiforth::task::TaskGroup & group, const tiforth::task::TaskContext & task_ctx)
{
    if (!group.GetTask())
        return arrow::Status::Invalid("task group must have a task");
    if (group.NumTasks() == 0)
        return arrow::Status::Invalid("task group num_tasks must be positive");

    for (tiforth::task::TaskId task_id = 0; task_id < group.NumTasks(); ++task_id)
        ARROW_RETURN_NOT_OK(RunTaskToCompletion(group.GetTask(), task_ctx, task_id));

    if (group.GetContinuation().has_value())
    {
        const auto & cont = *group.GetContinuation();
        while (true)
        {
            ARROW_ASSIGN_OR_RAISE(auto st, cont(task_ctx));
            if (st.IsFinished())
                break;
            if (st.IsContinue())
                continue;
            if (st.IsYield())
            {
                std::this_thread::yield();
                continue;
            }
            if (st.IsCancelled())
                return arrow::Status::Cancelled("continuation cancelled");
            if (st.IsBlocked())
            {
                ARROW_ASSIGN_OR_RAISE(const auto drive, DriveAwaiter(st.GetAwaiter()));
                if (drive == AwaiterDriveResult::kWaitingExternal)
                    return arrow::Status::Invalid("continuation blocked on external resumer");
                continue;
            }
            return arrow::Status::Invalid("unknown continuation status");
        }
    }

    return group.NotifyFinish(task_ctx);
}

inline arrow::Status RunTaskGroupsToCompletion(const tiforth::task::TaskGroups & groups, const tiforth::task::TaskContext & task_ctx)
{
    for (const auto & group : groups)
        ARROW_RETURN_NOT_OK(RunTaskGroupToCompletion(group, task_ctx));
    return arrow::Status::OK();
}

} // namespace DB::TiForth

#endif // defined(TIFLASH_ENABLE_TIFORTH)

