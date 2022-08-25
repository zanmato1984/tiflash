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

#include "Flash/Mpp/MPPTaskManager.h"
#include "Storages/Transaction/TMTContext.h"
#include "TestUtils/TiFlashTestEnv.h"
#include <Debug/MockStorage.h>
#include <Server/FlashGrpcServerHolder.h>

#include <unordered_map>

namespace DB::tests
{

/** Hold Mock Compute Server to manage the lifetime of them.
  * Maintains Mock Compute Server info.
  */
class MockComputeServerManager : public ext::Singleton<MockComputeServerManager>
{
public:
    /// register an server to run.
    void addServer(String addr);

    /// call startServers to run all servers in current test.
    void startServers(const LoggerPtr & log_ptr, Context & global_context);

    void startServers(const LoggerPtr & log_ptr, int start_idx);

    /// set MockStorage for Compute Server in order to mock input columns.
    void setMockStorage(MockStorage & mock_storage);

    /// stop all servers.
    void reset();

    MockMPPServerInfo getMockMPPServerInfo(size_t partition_id);

    std::unordered_map<size_t, MockServerConfig> & getServerConfigMap();

    void resetMockMPPServerInfo(size_t partition_num);

    void cancelTest()
    {
        std::cout << "ywq test server num: " << server_map.size() << std::endl;
        mpp::CancelTaskRequest req;
        auto * meta = req.mutable_meta();
        meta->set_start_ts(1);
        mpp::CancelTaskResponse response;
        showTaskInfo();
        for (const auto & server : server_map)
            server.second->getFlashService()->cancelMPPTaskForTest(&req, &response);

        showTaskInfo();
    }

    void showTaskInfo()
    {
        // ywq todo know the current context in use...
        for (int i = 0; i < TiFlashTestEnv::globalContextSize(); i++)
        {
            std::cout << TiFlashTestEnv::getGlobalContext(i).getTMTContext().getMPPTaskManager()->toString() << std::endl;
        }
    }

private:
    void addServer(size_t partition_id, std::unique_ptr<FlashGrpcServerHolder> server);
    void prepareMockMPPServerInfo();

private:
    std::unordered_map<size_t, std::unique_ptr<FlashGrpcServerHolder>> server_map;
    std::unordered_map<size_t, MockServerConfig> server_config_map;
};
} // namespace DB::tests