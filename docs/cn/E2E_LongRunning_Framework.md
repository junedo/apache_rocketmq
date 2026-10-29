<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# RocketMQ E2E 独立进程集群与 LongRunning 正确性验证框架

## 1. 背景与目标

在对 RocketMQ 存储引擎（如 CommitLog、ConsumeQueue、RocksDB 存储引擎、Timer/Transaction/Pop 状态存储）或 Broker 核心链路进行改造升级后，传统的单模块单元测试难以覆盖跨进程通信、多消息类型并发混合负载以及进程级故障重启（Crash / Rolling Restart）下的端到端不变量。

本框架提供了一套 **独立 OS 进程级集群编排脚本 (`test/bin/e2e-longrunning.sh`)** 与 **多模型端到端正确性验证驱动引擎 (`org.apache.rocketmq.test.longrunning.LongRunningTestDriver`)**，用于自动化验证：
1. **五类核心消息工作负载并发运行**：Normal 普通消息、FIFO 严格顺序消息、Delay/Timer 定时消息、Transaction 分布式事务消息（含本地 Commit/Rollback 与 Broker 事务回查 Commit/Rollback）、Pop 消费模式。
2. **五项端到端正确性不变量（Invariants）**：零消息丢失、零 Payload 篡改（CRC32 全量校验）、严格分区顺序性、事务回滚零泄露、定时消息触发精度。
3. **周期性独立进程故障注入（Chaos Fault Injection）**：在读写压测过程中周期性对独立 Broker 进程执行优雅重启（`SIGTERM`）或强制杀进程（`SIGKILL`），验证 WAL/Checkpoint 截断恢复与客户端自动重平衡后的端到端正确性。
4. **多存储引擎矩阵对比（Storage Matrix）**：支持一键运行 `default`（传统文件存储）、`rocksdb`（RocksDB CQ + Config V2 + Timer/Trans/Index RocksDB）以及 `matrix`（双模式自动对比）。

---

## 2. 架构与核心组件

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                     test/bin/e2e-longrunning.sh (集群编排与矩阵执行器)               │
│  --profile [default | rocksdb | matrix]  --duration 60  --chaos-interval 15       │
└───────────────┬───────────────────────────────────────────────────┬───────────────┘
                │ 启动/管理独立 OS 进程                               │ 启动独立验证 JVM 进程
                ▼                                                   ▼
┌───────────────────────────────────────────┐   ┌───────────────────────────────────┐
│        独立进程 RocketMQ 集群环境            │   │    LongRunning 端到端验证引擎       │
│                                           │   │   (LongRunningTestDriver CLI)     │
│  ┌─────────────────────────────────────┐  │   │                                   │
│  │ NamesrvStartup Process              │  │   │ 1. Workload 并发负载生成器          │
│  └─────────────────────────────────────┘  │   │    - NormalMessageWorkload        │
│  ┌─────────────────────────────────────┐  │◄──┤    - FifoOrderedWorkload          │
│  │ BrokerStartup Node-0 Process        │  │   │    - DelayTimerWorkload           │
│  │  ├─ Profile: default (File CQ/V1)   │  │   │    - TransactionWorkload          │
│  │  └─ Profile: rocksdb (RocksDB/V2)   │  │   │    - PopMessageWorkload           │
│  └─────────────────────────────────────┘  │   │                                   │
│  ┌─────────────────────────────────────┐  │   │ 2. CorrectnessLedger (真理账本)     │
│  │ BrokerStartup Node-1 Process        │  │   │    - 0 丢失 / 0 篡改(CRC32)        │
│  └─────────────────────────────────────┘  │   │    - 0 乱序 / 0 回滚消息泄露        │
│                     ▲                     │   │                                   │
└─────────────────────┼─────────────────────┘   │ 3. ClusterChaosController         │
                      └─────────────────────────┤    - 周期性重启独立 Broker 进程      │
                         SIGTERM / SIGKILL 重启  └───────────────────────────────────┘
```

### 代码结构
- `test/src/main/java/org/apache/rocketmq/test/longrunning/`
  - `LongRunningTestDriver.java`：独立进程入口，负责集群就绪探活、Topic/SubscriptionGroup 初始化、五大 Workload 生命周期编排、Chaos 故障控制器调度、排空等待（Drain Phase）与 JSON 报告生成。
  - `config/LongRunningConfig.java` & `StorageProfile.java`：命令行参数解析与存储引擎配置枚举。
  - `ledger/CorrectnessLedger.java` & `VerificationReport.java`：线程安全的端到端真理账本，实时校验每条消息的 CRC32、顺序性、事务状态与定时精度，并统计 P50/P95/P99/Max 端到端延迟。
  - `workload/NormalMessageWorkload.java`：普通消息并发发送与 Push 消费校验。
  - `workload/FifoOrderedWorkload.java`：基于固定队列选择器（Pinned Queue Selector）与 `MessageListenerOrderly` 的严格顺序消息校验。
  - `workload/DelayTimerWorkload.java`：基于 `deliverTimeMs` 的定时消息发送与投递时间戳校验。
  - `workload/TransactionWorkload.java`：覆盖直接 `COMMIT`（70%）、直接 `ROLLBACK`（20%）及 `UNKNOW` 触发 Broker 回查 `CHECK_COMMIT` / `CHECK_ROLLBACK`（10%）的全路径事务校验。
  - `workload/PopMessageWorkload.java`：基于 `RMQPopClient` 的 Pop 拉取与异步 `Ack` 闭环校验。
  - `workload/ClusterChaosController.java`：周期性调用集群脚本重启指定 Broker 进程并验证路由恢复。

---

## 3. 快速使用指南

### 3.1 一键运行 Default + RocksDB 双存储引擎矩阵对比验证

```bash
bash test/bin/e2e-longrunning.sh run \
  --profile matrix \
  --duration 30 \
  --drain-timeout 30 \
  --chaos-interval 10 \
  --chaos-mode graceful \
  --send-interval-ms 25 \
  --work-dir /tmp/rmq-e2e-longrunning
```

### 3.2 单独运行 RocksDB 全家桶模式（含 Kill 故障注入）

```bash
bash test/bin/e2e-longrunning.sh run \
  --profile rocksdb \
  --duration 60 \
  --chaos-interval 15 \
  --chaos-mode mixed \
  --work-dir /tmp/rmq-e2e-rocksdb
```

### 3.3 独立管理集群生命周期（手工调试）

```bash
# 启动独立进程集群 (NameServer + 2 Brokers)
bash test/bin/e2e-longrunning.sh start-cluster --profile rocksdb --work-dir /tmp/rmq-cluster

# 查看各节点 OS 进程状态
bash test/bin/e2e-longrunning.sh status --work-dir /tmp/rmq-cluster

# 手动触发单个 Broker 节点重启 (graceful 或 kill)
bash test/bin/e2e-longrunning.sh restart-broker --work-dir /tmp/rmq-cluster --broker-id 0 --mode graceful

# 停止集群
bash test/bin/e2e-longrunning.sh stop-cluster --work-dir /tmp/rmq-cluster
```

---

## 4. 验证报告示例 (`verification-report-<profile>.json`)

每次运行结束后，框架会在 `--work-dir` 下自动生成结构化 JSON 验证报告，只有当以下所有条件同时满足时 `passed` 才为 `true`（否则进程以非零退出码返回）：
- `unconsumedCount == 0`（零消息丢失）
- `crcCorruptionCount == 0`（零消息体损坏）
- `fifoOrderViolationCount == 0 && fifoGapViolationCount == 0`（零乱序、零跳号）
- `rollbackLeakCount == 0`（零回滚事务消息泄露）
- `prematureTimerDeliveryCount == 0`（零定时消息提前投递）
- `chaosEventsTriggered == chaosEventsSucceeded`（所有故障注入轮次 100% 恢复成功）
