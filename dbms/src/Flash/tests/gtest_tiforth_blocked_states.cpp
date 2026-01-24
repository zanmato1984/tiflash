#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/TiForth/BlockPipelineRunner.h>
#include <Flash/TiForth/TiForthPipelineBlockInputStream.h>

#include <arrow/memory_pool.h>
#include <arrow/status.h>

#include <atomic>
#include <unordered_map>
#include <vector>

#include "tiforth/engine.h"
#include "tiforth/pipeline/op/op.h"
#include "tiforth/task/blocked_resumer.h"

namespace DB::tests
{
namespace
{

enum class PilotBlockKind
{
    kIOIn,
    kIOOut,
    kWaiting,
    kWaitForNotify,
};

enum class PilotErrorPoint
{
    kNone,
    kPipe,
    kExecuteIO,
    kAwait,
    kNotify,
};

struct PilotAsyncOptions
{
    PilotBlockKind block_kind = PilotBlockKind::kIOIn;
    int32_t block_cycles = 1;

    PilotErrorPoint error_point = PilotErrorPoint::kNone;
    arrow::Status error_status = arrow::Status::IOError("pilot operator error");
};

class PilotAsyncPipeOp final : public tiforth::pipeline::PipeOp
{
public:
    explicit PilotAsyncPipeOp(PilotAsyncOptions options)
        : options_(std::move(options))
    {
        if (options_.block_cycles <= 0)
            options_.block_cycles = 1;
    }

    tiforth::pipeline::PipelinePipe Pipe(const tiforth::pipeline::PipelineContext &) override
    {
        return [this](const tiforth::pipeline::PipelineContext &,
                      const tiforth::task::TaskContext &,
                      tiforth::pipeline::ThreadId,
                      std::optional<tiforth::pipeline::Batch> input) -> tiforth::pipeline::OpResult {
            if (options_.error_point == PilotErrorPoint::kPipe)
                return options_.error_status;

            if (!input.has_value())
            {
                if (buffered_output_ != nullptr)
                {
                    auto out = std::move(buffered_output_);
                    buffered_output_.reset();
                    return tiforth::pipeline::OpOutput::PipeEven(std::move(out));
                }
                return tiforth::pipeline::OpOutput::PipeSinkNeedsMore();
            }

            if (state_ != State::kIdle)
                return arrow::Status::Invalid("pilot operator is not idle");

            auto batch = std::move(*input);
            if (batch == nullptr)
                return arrow::Status::Invalid("pilot input batch must not be null");

            pending_input_ = std::move(batch);
            remaining_block_cycles_ = options_.block_cycles;

            switch (options_.block_kind)
            {
            case PilotBlockKind::kIOIn:
                state_ = State::kBlockedIO;
                return tiforth::pipeline::OpOutput::Blocked(
                    std::make_shared<PilotResumer>(this, tiforth::task::BlockedKind::kIOIn));
            case PilotBlockKind::kIOOut:
                state_ = State::kBlockedIO;
                return tiforth::pipeline::OpOutput::Blocked(
                    std::make_shared<PilotResumer>(this, tiforth::task::BlockedKind::kIOOut));
            case PilotBlockKind::kWaiting:
                state_ = State::kBlockedWait;
                return tiforth::pipeline::OpOutput::Blocked(
                    std::make_shared<PilotResumer>(this, tiforth::task::BlockedKind::kWaiting));
            case PilotBlockKind::kWaitForNotify:
                state_ = State::kWaitForNotify;
                return tiforth::pipeline::OpOutput::Blocked(
                    std::make_shared<PilotResumer>(this, tiforth::task::BlockedKind::kWaitForNotify));
            }
            return arrow::Status::Invalid("unknown pilot block kind");
        };
    }

    tiforth::pipeline::PipelineDrain Drain(const tiforth::pipeline::PipelineContext &) override
    {
        return [](const tiforth::pipeline::PipelineContext &,
                  const tiforth::task::TaskContext &,
                  tiforth::pipeline::ThreadId) -> tiforth::pipeline::OpResult {
            return tiforth::pipeline::OpOutput::Finished();
        };
    }

private:
    enum class State
    {
        kIdle,
        kBlockedIO,
        kBlockedWait,
        kWaitForNotify,
    };

    class PilotResumer final : public tiforth::task::BlockedResumer
    {
    public:
        PilotResumer(PilotAsyncPipeOp * op, tiforth::task::BlockedKind kind)
            : op_(op)
            , kind_(kind)
        {}

        void Resume() override { resumed_.store(true, std::memory_order_release); }
        bool IsResumed() const override { return resumed_.load(std::memory_order_acquire); }

        tiforth::task::BlockedKind kind() const override { return kind_; }

        arrow::Result<std::optional<tiforth::task::BlockedKind>> ExecuteIO() override
        {
            if (op_ == nullptr)
                return arrow::Status::Invalid("pilot resumer has null operator");
            if (op_->options_.error_point == PilotErrorPoint::kExecuteIO)
                return op_->options_.error_status;
            if (op_->state_ != State::kBlockedIO)
                return arrow::Status::Invalid("pilot operator is not in IO state");
            if (kind_ != tiforth::task::BlockedKind::kIOIn && kind_ != tiforth::task::BlockedKind::kIOOut)
                return arrow::Status::Invalid("pilot resumer kind is not IO");
            if (op_->remaining_block_cycles_ <= 0)
                return arrow::Status::Invalid("pilot operator IO cycle counter underflow");
            if (--op_->remaining_block_cycles_ > 0)
                return kind_;
            op_->BufferAndReset();
            return std::nullopt;
        }

        arrow::Result<std::optional<tiforth::task::BlockedKind>> Await() override
        {
            if (op_ == nullptr)
                return arrow::Status::Invalid("pilot resumer has null operator");
            if (op_->options_.error_point == PilotErrorPoint::kAwait)
                return op_->options_.error_status;
            if (op_->state_ != State::kBlockedWait)
                return arrow::Status::Invalid("pilot operator is not in await state");
            if (kind_ != tiforth::task::BlockedKind::kWaiting)
                return arrow::Status::Invalid("pilot resumer kind is not Waiting");
            if (op_->remaining_block_cycles_ <= 0)
                return arrow::Status::Invalid("pilot operator await cycle counter underflow");
            if (--op_->remaining_block_cycles_ > 0)
                return kind_;
            op_->BufferAndReset();
            return std::nullopt;
        }

        arrow::Status Notify() override
        {
            if (op_ == nullptr)
                return arrow::Status::Invalid("pilot resumer has null operator");
            if (op_->options_.error_point == PilotErrorPoint::kNotify)
                return op_->options_.error_status;
            if (op_->state_ != State::kWaitForNotify)
                return arrow::Status::Invalid("pilot operator is not waiting for notify");
            if (kind_ != tiforth::task::BlockedKind::kWaitForNotify)
                return arrow::Status::Invalid("pilot resumer kind is not WaitForNotify");
            op_->BufferAndReset();
            return arrow::Status::OK();
        }

    private:
        PilotAsyncPipeOp * op_ = nullptr;
        tiforth::task::BlockedKind kind_;
        std::atomic_bool resumed_{false};
    };

    void BufferAndReset()
    {
        buffered_output_ = std::move(pending_input_);
        pending_input_.reset();
        remaining_block_cycles_ = 0;
        state_ = State::kIdle;
    }

    PilotAsyncOptions options_;
    State state_ = State::kIdle;

    int32_t remaining_block_cycles_ = 0;
    std::shared_ptr<arrow::RecordBatch> pending_input_;
    std::shared_ptr<arrow::RecordBatch> buffered_output_;
};

Block makeInt64Block(std::initializer_list<Int64> values)
{
    auto col = ColumnInt64::create();
    col->reserve(values.size());
    for (const auto v : values)
        col->insert(v);
    auto type = std::make_shared<DataTypeInt64>();

    ColumnsWithTypeAndName cols;
    cols.emplace_back(std::move(col), type, "col0");
    return Block(std::move(cols));
}

std::vector<Int64> flattenInt64Column(const std::vector<Block> & blocks)
{
    std::vector<Int64> out;
    for (const auto & block : blocks)
    {
        const auto * col = typeid_cast<const ColumnInt64 *>(block.getByName("col0").column.get());
        if (col == nullptr)
            return {};
        const auto & data = col->getData();
        out.insert(out.end(), data.begin(), data.end());
    }
    return out;
}

std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> makePilotPipeOps(
    PilotBlockKind block_kind,
    int32_t block_cycles)
{
    PilotAsyncOptions options;
    options.block_kind = block_kind;
    options.block_cycles = block_cycles;

    std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops;
    pipe_ops.push_back(std::make_unique<PilotAsyncPipeOp>(options));
    return pipe_ops;
}

void runPilotOnBlocks(PilotBlockKind kind)
{
    auto pool_holder = std::shared_ptr<arrow::MemoryPool>(arrow::default_memory_pool(), [](arrow::MemoryPool *) {});

    tiforth::EngineOptions engine_options;
    engine_options.memory_pool = pool_holder.get();
    auto engine_res = tiforth::Engine::Create(engine_options);
    ASSERT_TRUE(engine_res.ok()) << engine_res.status().ToString();
    auto engine = std::move(engine_res).ValueOrDie();
    ASSERT_NE(engine, nullptr);

    std::vector<Block> input_blocks;
    input_blocks.push_back(makeInt64Block({0, 1, 2}));
    input_blocks.push_back(makeInt64Block({3, 4}));

    auto outputs_res = DB::TiForth::RunTiForthPipeOpsOnBlocks(
        makePilotPipeOps(kind, /*block_cycles=*/3),
        input_blocks,
        /*input_options_by_name=*/std::unordered_map<String, DB::TiForth::ColumnOptions>{},
        pool_holder.get());
    ASSERT_TRUE(outputs_res.ok()) << outputs_res.status().ToString();
    auto outputs = std::move(outputs_res).ValueOrDie();

    std::vector<Block> out_blocks;
    out_blocks.reserve(outputs.size());
    for (auto & out : outputs)
        out_blocks.push_back(std::move(out.block));

    EXPECT_EQ(flattenInt64Column(out_blocks), (std::vector<Int64>{0, 1, 2, 3, 4}));
}

void runPilotOnAggBlockInputStream(PilotBlockKind kind)
{
    auto pool_holder = std::shared_ptr<arrow::MemoryPool>(arrow::default_memory_pool(), [](arrow::MemoryPool *) {});

    tiforth::EngineOptions engine_options;
    engine_options.memory_pool = pool_holder.get();
    auto engine_res = tiforth::Engine::Create(engine_options);
    ASSERT_TRUE(engine_res.ok()) << engine_res.status().ToString();
    auto engine = std::move(engine_res).ValueOrDie();
    ASSERT_NE(engine, nullptr);

    auto pipe_ops = makePilotPipeOps(kind, /*block_cycles=*/3);

    BlocksList blocks;
    blocks.push_back(makeInt64Block({0, 1, 2}));
    blocks.push_back(makeInt64Block({3, 4}));
    auto input_stream = std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
    ASSERT_NE(input_stream, nullptr);

    NamesAndTypesList output_columns;
    output_columns.emplace_back("col0", std::make_shared<DataTypeInt64>());

    const Block sample_input_block = input_stream->getHeader();
    auto stream = std::make_shared<DB::TiForth::TiForthPipelineBlockInputStream>(
        "TiForthPilotBlockedStates",
        input_stream,
        std::move(engine),
        std::move(pipe_ops),
        output_columns,
        /*input_options_by_name=*/std::unordered_map<String, DB::TiForth::ColumnOptions>{},
        pool_holder,
        sample_input_block);
    ASSERT_NE(stream, nullptr);

    stream->readPrefix();
    std::vector<Block> out_blocks;
    while (Block out = stream->read())
        out_blocks.push_back(std::move(out));
    stream->readSuffix();

    EXPECT_EQ(flattenInt64Column(out_blocks), (std::vector<Int64>{0, 1, 2, 3, 4}));
}

} // namespace

TEST(TiForthBlockedStatesTest, PilotIOIn)
{
    runPilotOnBlocks(PilotBlockKind::kIOIn);
    runPilotOnAggBlockInputStream(PilotBlockKind::kIOIn);
}

TEST(TiForthBlockedStatesTest, PilotIOOut)
{
    runPilotOnBlocks(PilotBlockKind::kIOOut);
    runPilotOnAggBlockInputStream(PilotBlockKind::kIOOut);
}

TEST(TiForthBlockedStatesTest, PilotWaiting)
{
    runPilotOnBlocks(PilotBlockKind::kWaiting);
    runPilotOnAggBlockInputStream(PilotBlockKind::kWaiting);
}

TEST(TiForthBlockedStatesTest, PilotWaitForNotify)
{
    runPilotOnBlocks(PilotBlockKind::kWaitForNotify);
    runPilotOnAggBlockInputStream(PilotBlockKind::kWaitForNotify);
}

} // namespace DB::tests

#else

TEST(TiForthBlockedStatesTest, Disabled)
{
    GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
