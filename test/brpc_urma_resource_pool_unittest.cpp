// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations

#include <gtest/gtest.h>

#if BRPC_WITH_URMA

#include <dirent.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <set>
#include <vector>

#include "brpc/urma/urma_resource.h"
#include "urma_api.h"

namespace brpc {
namespace urma {
namespace {

class UrmaResourcePoolTest : public testing::Test {
protected:
    void SetUp() override {
        urma_init_attr_t init_attr{};
        const auto status = urma_init(&init_attr);
        if (status != URMA_SUCCESS && status != URMA_EEXIST) {
            GTEST_SKIP() << "Requires the link-time URMA mock";
        }
        _owns_init = status == URMA_SUCCESS;

        int count = 0;
        auto** devices = urma_get_device_list(&count);
        auto* device = devices && count > 0 ? devices[0] : nullptr;
        urma_free_device_list(devices);
        if (!device || std::strcmp(device->name, "mock_urma_device") != 0) {
            GTEST_SKIP() << "Requires the link-time URMA mock";
        }

        urma_device_attr_t attr{};
        ASSERT_EQ(URMA_SUCCESS, urma_query_device(device, &attr));
        _caps = attr.dev_cap;
        _context = urma_create_context(device, 0);
        ASSERT_NE(nullptr, _context);
    }

    void TearDown() override {
        if (_context) {
            EXPECT_EQ(URMA_SUCCESS, urma_delete_context(_context));
        }
        if (_owns_init) {
            EXPECT_EQ(URMA_SUCCESS, urma_uninit());
        }
    }

    SharedQueueConfig Config() const {
        return {_context, 2, 3, 32, 64, 4, 2, 1};
    }

    urma_context_t* _context{nullptr};
    urma_device_cap_t _caps{};

private:
    bool _owns_init{false};
};

TEST_F(UrmaResourcePoolTest, CreatesExclusiveQueuesWithRequestedConfig) {
    for (bool polling : {false, true}) {
        const LocalJettyConfig config{_context, 32, 64, 2, 1, polling};
        auto queues = LocalJettyResources::Create(config);
        ASSERT_NE(nullptr, queues);
        EXPECT_TRUE(config == queues->config);
        EXPECT_EQ(96u, queues->jfc->jfc_cfg.depth);
        EXPECT_EQ(32u, queues->jetty->jetty_cfg.jfs_cfg.depth);
        EXPECT_EQ(2u, queues->jetty->jetty_cfg.jfs_cfg.max_sge);
        EXPECT_EQ(1u, queues->jetty->jetty_cfg.jfs_cfg.priority);
        EXPECT_EQ(queues->jfr, queues->jetty->jetty_cfg.shared.jfr);
        EXPECT_EQ(queues->jfc, queues->jetty->jetty_cfg.shared.jfc);

        auto different_depth = config;
        ++different_depth.rq_depth;
        EXPECT_FALSE(different_depth == queues->config);
        auto different_mode = config;
        different_mode.use_polling = !polling;
        EXPECT_FALSE(different_mode == queues->config);

        const int fd = queues->jfce->fd;
        ASSERT_GE(fcntl(fd, F_GETFD), 0);
        queues.reset();
        EXPECT_EQ(-1, fcntl(fd, F_GETFD));
        EXPECT_EQ(EBADF, errno);
    }
}

TEST_F(UrmaResourcePoolTest, SharesQueueSlotsAndKeepsThemAliveForLeases) {
    const auto config = Config();
    auto pool = SharedQueuePool::Create(config, _caps);
    ASSERT_NE(nullptr, pool);

    std::set<urma_jfs_t*> send_queues;
    std::set<urma_jfr_t*> recv_queues;
    std::set<urma_jfc_t*> completion_queues;
    std::vector<SharedQueuePool::Lease> leases;
    for (int i = 0; i < 64; ++i) {
        auto lease = pool->Acquire();
        ASSERT_NE(nullptr, lease.send->jfs());
        ASSERT_NE(nullptr, lease.recv->jfr());
        EXPECT_EQ(nullptr, lease.send->jfr());
        EXPECT_EQ(nullptr, lease.recv->jfs());
        EXPECT_EQ(config.send_depth, lease.send->jfc->jfc_cfg.depth);
        EXPECT_EQ(config.recv_depth, lease.recv->jfc->jfc_cfg.depth);
        EXPECT_EQ(config.send_max_sge, lease.send->jfs()->jfs_cfg.max_sge);
        send_queues.insert(lease.send->jfs());
        recv_queues.insert(lease.recv->jfr());
        completion_queues.insert(lease.send->jfc);
        completion_queues.insert(lease.recv->jfc);
        leases.push_back(std::move(lease));
    }
    EXPECT_EQ(config.send_count, send_queues.size());
    EXPECT_EQ(config.recv_count, recv_queues.size());
    EXPECT_EQ(config.send_count + config.recv_count, completion_queues.size());

    const int send_fd = leases[0].send->jfce->fd;
    const int recv_fd = leases[0].recv->jfce->fd;
    pool.reset();
    EXPECT_GE(fcntl(send_fd, F_GETFD), 0);
    EXPECT_GE(fcntl(recv_fd, F_GETFD), 0);
    leases.clear();
    EXPECT_EQ(-1, fcntl(send_fd, F_GETFD));
    EXPECT_EQ(EBADF, errno);
    EXPECT_EQ(-1, fcntl(recv_fd, F_GETFD));
    EXPECT_EQ(EBADF, errno);
}

TEST_F(UrmaResourcePoolTest, EnforcesDeviceBudgetAndRejectsEmptyPool) {
    auto config = Config();
    auto caps = _caps;
    caps.max_jfc = config.send_count + config.recv_count - 1;
    EXPECT_EQ(nullptr, SharedQueuePool::Create(config, caps));
    EXPECT_EQ(ENOSPC, errno);

    config.recv_count = 0;
    EXPECT_EQ(nullptr, SharedQueuePool::Create(config, _caps));
    EXPECT_EQ(EINVAL, errno);
}

int CountFileDescriptors() {
    DIR* dir = opendir("/proc/self/fd");
    if (!dir) {
        return -1;
    }
    int count = 0;
    while (readdir(dir)) {
        ++count;
    }
    closedir(dir);
    return count;
}

TEST_F(UrmaResourcePoolTest, CleansUpAllCqsAfterProviderRejectsReceiveQueue) {
    auto config = Config();
    auto caps = _caps;
    // Simulate a provider rejecting a receive queue after both send queues
    // and the first receive CQ were created.
    config.recv_depth = _caps.max_jfr_depth + 1;
    caps.max_jfr_depth = config.recv_depth;
    const int before = CountFileDescriptors();
    ASSERT_GT(before, 0);
    EXPECT_EQ(nullptr, SharedQueuePool::Create(config, caps));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(before, CountFileDescriptors());
}

TEST_F(UrmaResourcePoolTest, PreparedJettyIsOnlyTakenByMatchingConfig) {
    PreparedJettyPool pool;
    const LocalJettyConfig config{_context, 32, 64, 2, 1, false};
    pool.Prepare(config, 1);

    auto different = config;
    ++different.rq_depth;
    auto cold = pool.Acquire(different);
    auto prepared = pool.Acquire(config);
    auto second = pool.Acquire(config);
    ASSERT_NE(nullptr, cold);
    ASSERT_NE(nullptr, prepared);
    ASSERT_NE(nullptr, second);
    EXPECT_TRUE(different == cold->config);
    EXPECT_TRUE(config == prepared->config);
    EXPECT_NE(prepared->jetty, second->jetty);
    // Mock Jetty IDs grow in creation order. The prepared one predates the
    // cold allocation even though it was acquired afterward.
    EXPECT_LT(prepared->jetty->jetty_id.id, cold->jetty->jetty_id.id);
    EXPECT_LT(cold->jetty->jetty_id.id, second->jetty->jetty_id.id);
}

}  // namespace
}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA
