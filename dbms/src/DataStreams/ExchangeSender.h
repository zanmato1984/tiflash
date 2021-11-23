#pragma once

#include <Common/LogWithPrefix.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <tipb/executor.pb.h>

namespace DB
{
/// read blocks directly from Union, then broadcast or partition blocks and encode them, later put them into sending tunnels
/// it should be an output stream but derived from the inputSteam
class ExchangeSender : public IProfilingBlockInputStream
{
public:
    ExchangeSender(const tipb::ExchangeSender & exs, const BlockInputStreamPtr & input, std::unique_ptr<DAGResponseWriter> writer, const std::shared_ptr<LogWithPrefix> & log_ = nullptr)
        : pb_exchange_sender(exs)
        , writer(std::move(writer))
        , log(getLogWithPrefix(log_, getName()))
    {
        children.push_back(input);
    }
    String getName() const override { return "ExchangeSender"; }
    Block getHeader() const override { return children.back()->getHeader(); }
    void readSuffix() override
    {
        writer->finishWrite();
    }

    const tipb::ExchangeSender pb_exchange_sender;

protected:
    Block readImpl() override;

private:
    std::unique_ptr<DAGResponseWriter> writer;
    std::shared_ptr<LogWithPrefix> log;
};

} // namespace DB
