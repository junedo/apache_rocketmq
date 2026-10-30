/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.test.longrunning;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.test.longrunning.config.LongRunningConfig;
import org.apache.rocketmq.test.longrunning.ledger.CorrectnessLedger;
import org.apache.rocketmq.test.longrunning.ledger.VerificationReport;
import org.apache.rocketmq.test.longrunning.suite.E2EScenarioSuite;
import org.apache.rocketmq.test.longrunning.workload.BatchMessageWorkload;
import org.apache.rocketmq.test.longrunning.workload.ClusterChaosController;
import org.apache.rocketmq.test.longrunning.workload.DelayTimerWorkload;
import org.apache.rocketmq.test.longrunning.workload.FifoOrderedWorkload;
import org.apache.rocketmq.test.longrunning.workload.NormalMessageWorkload;
import org.apache.rocketmq.test.longrunning.workload.PopMessageWorkload;
import org.apache.rocketmq.test.longrunning.workload.RetryAndLitePullWorkload;
import org.apache.rocketmq.test.longrunning.workload.TagAndSqlFilterWorkload;
import org.apache.rocketmq.test.longrunning.workload.TransactionWorkload;
import org.apache.rocketmq.test.longrunning.workload.Workload;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

public class LongRunningTestDriver {

    private static final Logger log = LoggerFactory.getLogger(LongRunningTestDriver.class);

    private final LongRunningConfig config;
    private final CorrectnessLedger ledger;
    private final List<Workload> workloads = new ArrayList<>();

    public LongRunningTestDriver(LongRunningConfig config) {
        this.config = config;
        this.ledger = new CorrectnessLedger(config);
    }

    public VerificationReport execute() throws Exception {
        long startTimeMs = System.currentTimeMillis();
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt("lr-driver-admin-" + UUID.randomUUID());
        adminExt.setNamesrvAddr(config.getNamesrvAddr());
        adminExt.start();

        ClusterChaosController chaosController = null;
        try {
            int brokerCount = waitForClusterReady(adminExt, 30000L);
            log.info("Cluster [{}] is ready on NameServer [{}] with {} broker(s), profile={}",
                config.getClusterName(), config.getNamesrvAddr(), brokerCount, config.getProfile().getName());

            if (config.isEnableE2EScenarioSuite()) {
                E2EScenarioSuite suite = new E2EScenarioSuite(config, ledger, adminExt);
                suite.runAllScenarios();
            }

            if (config.getDurationSeconds() > 0) {
                registerWorkloads(adminExt);

                for (Workload workload : workloads) {
                    workload.start();
                }

                chaosController = new ClusterChaosController(config, ledger, adminExt, brokerCount);
                chaosController.start();

                long endRunTimeMs = System.currentTimeMillis() + config.getDurationSeconds() * 1000L;
                while (System.currentTimeMillis() < endRunTimeMs) {
                    long remainingSec = Math.max(0L, (endRunTimeMs - System.currentTimeMillis()) / 1000L);
                    log.info("[Progress runId={} profile={} remaining={}s] pending={}",
                        config.getRunId(), config.getProfile().getName(), remainingSec, ledger.getPendingCount());
                    Thread.sleep(Math.min(2000L, Math.max(200L, endRunTimeMs - System.currentTimeMillis())));
                }

                // 1. Stop chaos fault injection first and ensure all brokers are back online
                chaosController.stop();
                waitForClusterReady(adminExt, 30000L);

                // 2. Stop all producers
                log.info("Stopping producers and entering drain phase (timeout={}s)...", config.getDrainTimeoutSeconds());
                for (Workload workload : workloads) {
                    workload.stopProducing();
                }

                // 3. Wait for all expected messages (including delay & transaction check-backs) to drain
                long drainWaitMs = Math.max(config.getDrainTimeoutSeconds() * 1000L, config.getDelayMessageMs() + 10000L);
                boolean drained = ledger.awaitDrain(drainWaitMs);
                log.info("Drain completed: drained={}, remainingPending={}", drained, ledger.getPendingCount());
            }
        } finally {
            if (chaosController != null) {
                chaosController.stop();
            }
            for (Workload workload : workloads) {
                try {
                    workload.shutdown();
                } catch (Exception e) {
                    log.warn("Error shutting down workload {}", workload.name(), e);
                }
            }
            adminExt.shutdown();
        }

        long totalDurationMs = System.currentTimeMillis() - startTimeMs;
        VerificationReport report = ledger.buildReport(totalDurationMs);
        writeReportFiles(report);
        logReportSummary(report);
        return report;
    }

    private void registerWorkloads(DefaultMQAdminExt adminExt) throws Exception {
        if (config.isEnableNormalWorkload()) {
            NormalMessageWorkload w = new NormalMessageWorkload(config, ledger);
            createTopicOnCluster(adminExt, w.topicName(), config.getQueueNums(), new HashMap<>());
            workloads.add(w);
        }
        if (config.isEnableFifoWorkload()) {
            FifoOrderedWorkload w = new FifoOrderedWorkload(config, ledger);
            createTopicOnCluster(adminExt, w.topicName(), config.getQueueNums(), new HashMap<>());
            workloads.add(w);
        }
        if (config.isEnableDelayWorkload()) {
            DelayTimerWorkload w = new DelayTimerWorkload(config, ledger);
            createTopicOnCluster(adminExt, w.topicName(), config.getQueueNums(), new HashMap<>());
            workloads.add(w);
        }
        if (config.isEnableTransactionWorkload()) {
            TransactionWorkload w = new TransactionWorkload(config, ledger);
            createTopicOnCluster(adminExt, w.topicName(), config.getQueueNums(), new HashMap<>());
            workloads.add(w);
        }
        if (config.isEnablePopWorkload()) {
            PopMessageWorkload w = new PopMessageWorkload(config, ledger);
            createTopicOnCluster(adminExt, w.topicName(), config.getQueueNums(), new HashMap<>());
            createSubscriptionGroupOnCluster(adminExt, w.getConsumerGroup());
            workloads.add(w);
        }
        if (config.isEnableBatchWorkload()) {
            BatchMessageWorkload w = new BatchMessageWorkload(config, ledger);
            createTopicOnCluster(adminExt, w.topicName(), config.getQueueNums(), new HashMap<>());
            workloads.add(w);
        }
        if (config.isEnableFilterWorkload()) {
            TagAndSqlFilterWorkload w = new TagAndSqlFilterWorkload(config, ledger);
            createTopicOnCluster(adminExt, w.topicName(), config.getQueueNums(), new HashMap<>());
            workloads.add(w);
        }
        if (config.isEnableRetryPullWorkload()) {
            RetryAndLitePullWorkload w = new RetryAndLitePullWorkload(config, ledger);
            createTopicOnCluster(adminExt, w.topicName(), config.getQueueNums(), new HashMap<>());
            workloads.add(w);
        }
    }

    private int waitForClusterReady(DefaultMQAdminExt adminExt, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                ClusterInfo clusterInfo = adminExt.examineBrokerClusterInfo();
                if (clusterInfo != null && clusterInfo.getBrokerAddrTable() != null
                    && !clusterInfo.getBrokerAddrTable().isEmpty()) {
                    int activeMasters = 0;
                    for (BrokerData bd : clusterInfo.getBrokerAddrTable().values()) {
                        if (bd.getBrokerAddrs() != null && bd.getBrokerAddrs().containsKey(0L)) {
                            activeMasters++;
                        }
                    }
                    if (activeMasters > 0) {
                        return activeMasters;
                    }
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(500L);
        }
        throw new IllegalStateException("Cluster did not become ready within " + timeoutMs + " ms on " + config.getNamesrvAddr());
    }

    private void createTopicOnCluster(DefaultMQAdminExt adminExt, String topic,
        int queueNums, Map<String, String> attributes) throws Exception {
        Set<String> masterAddrs = CommandUtil.fetchMasterAddrByClusterName(adminExt, config.getClusterName());
        TopicConfig topicConfig = new TopicConfig(topic, queueNums, queueNums, PermName.PERM_READ | PermName.PERM_WRITE);
        topicConfig.setAttributes(attributes);
        for (String addr : masterAddrs) {
            adminExt.createAndUpdateTopicConfig(addr, topicConfig);
        }
        Thread.sleep(300L);
    }

    private void createSubscriptionGroupOnCluster(DefaultMQAdminExt adminExt, String groupName) throws Exception {
        Set<String> masterAddrs = CommandUtil.fetchMasterAddrByClusterName(adminExt, config.getClusterName());
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(groupName);
        groupConfig.setConsumeEnable(true);
        groupConfig.setConsumeFromMinEnable(true);
        for (String addr : masterAddrs) {
            adminExt.createAndUpdateSubscriptionGroupConfig(addr, groupConfig);
        }
    }

    private void writeReportFiles(VerificationReport report) {
        try {
            File dir = new File(config.getWorkDir());
            if (!dir.exists()) {
                dir.mkdirs();
            }
            File jsonFile = new File(dir, "verification-report-" + config.getProfile().getName() + ".json");
            String json = JSON.toJSONString(report, JSONWriter.Feature.PrettyFormat);
            Files.write(jsonFile.toPath(), json.getBytes(StandardCharsets.UTF_8));
            log.info("Wrote verification report to {}", jsonFile.getAbsolutePath());
        } catch (Exception e) {
            log.error("Failed to write verification report file", e);
        }
    }

    private void logReportSummary(VerificationReport report) {
        log.info("================================================================================");
        log.info("E2E LongRunning Verification Report [runId={}, profile={}, passed={}]",
            report.getRunId(), report.getProfile(), report.isPassed());
        log.info("  Duration              : {} ms", report.getDurationMs());
        log.info("  E2E Suite Cases       : {}/{} passed (failed={})",
            report.getE2eCasesPassed(), report.getE2eCasesTotal(), report.getE2eCasesFailed());
        log.info("  Expected Delivered    : {}", report.getTotalExpectedDelivered());
        log.info("  Unique Consumed       : {}", report.getTotalUniqueConsumed());
        log.info("  Duplicate Redeliveries: {}", report.getTotalDuplicateConsumed());
        log.info("  Tx Rolled Back        : {}", report.getTotalRolledBack());
        log.info("  Filtered Out Msgs     : {}", report.getTotalFilteredOut());
        log.info("  Unconsumed (Lost)     : {}", report.getUnconsumedCount());
        log.info("  CRC32 Corruptions     : {}", report.getCrcCorruptionCount());
        log.info("  FIFO Order Violations : {}", report.getFifoOrderViolationCount());
        log.info("  FIFO Sequence Gaps    : {}", report.getFifoGapViolationCount());
        log.info("  Tx Rollback Leaks     : {}", report.getRollbackLeakCount());
        log.info("  Filter Leaks          : {}", report.getFilterLeakCount());
        log.info("  Premature Timer Msgs  : {}", report.getPrematureTimerDeliveryCount());
        log.info("  Chaos Events (OK/All) : {}/{}", report.getChaosEventsSucceeded(), report.getChaosEventsTriggered());
        log.info("  Latency (P50/P95/P99) : {}/{}/{} ms (max={} ms)",
            report.getP50LatencyMs(), report.getP95LatencyMs(), report.getP99LatencyMs(), report.getMaxLatencyMs());
        log.info("  Sent by Workload      : {}", report.getSentByWorkload());
        log.info("  Consumed by Workload  : {}", report.getConsumedByWorkload());
        if (!report.getViolations().isEmpty()) {
            log.error("  Violations            : {}", report.getViolations());
        }
        log.info("================================================================================");
    }

    public static void main(String[] args) throws Exception {
        LongRunningConfig cfg = LongRunningConfig.fromArgs(args);
        LongRunningTestDriver driver = new LongRunningTestDriver(cfg);
        VerificationReport report = driver.execute();
        if (!report.isPassed()) {
            System.exit(1);
        }
        System.exit(0);
    }
}
