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
package org.apache.rocketmq.test.longrunning.workload;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.test.longrunning.config.LongRunningConfig;
import org.apache.rocketmq.test.longrunning.ledger.CorrectnessLedger;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class ClusterChaosController {

    private static final Logger log = LoggerFactory.getLogger(ClusterChaosController.class);

    private final LongRunningConfig config;
    private final CorrectnessLedger ledger;
    private final DefaultMQAdminExt adminExt;
    private final int expectedBrokerCount;
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final AtomicInteger roundCounter = new AtomicInteger(0);
    private ScheduledExecutorService scheduler;

    public ClusterChaosController(LongRunningConfig config, CorrectnessLedger ledger,
        DefaultMQAdminExt adminExt, int expectedBrokerCount) {
        this.config = config;
        this.ledger = ledger;
        this.adminExt = adminExt;
        this.expectedBrokerCount = expectedBrokerCount;
    }

    public void start() {
        if (config.getChaosIntervalSeconds() <= 0
            || config.getClusterScriptPath() == null
            || config.getClusterScriptPath().trim().isEmpty()) {
            log.info("Chaos injection is disabled (chaosIntervalSeconds={})", config.getChaosIntervalSeconds());
            return;
        }
        File scriptFile = new File(config.getClusterScriptPath());
        if (!scriptFile.exists()) {
            log.warn("Cluster script {} does not exist, skipping chaos injection", config.getClusterScriptPath());
            return;
        }
        active.set(true);
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ClusterChaosController");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(
            this::injectFaultAndVerifyRecovery,
            config.getChaosIntervalSeconds(),
            config.getChaosIntervalSeconds(),
            TimeUnit.SECONDS);
        log.info("Started ClusterChaosController with interval={}s, mode={}",
            config.getChaosIntervalSeconds(), config.getChaosMode());
    }

    private void injectFaultAndVerifyRecovery() {
        if (!active.get()) {
            return;
        }
        int round = roundCounter.getAndIncrement();
        int targetBrokerIdx = round % Math.max(1, expectedBrokerCount);
        String mode = config.getChaosMode();
        if ("mixed".equalsIgnoreCase(mode)) {
            mode = (round % 2 == 0) ? "graceful" : "kill";
        }

        long startMs = System.currentTimeMillis();
        log.info("[Chaos Round {}] Restarting broker-{} with mode={} ...", round, targetBrokerIdx, mode);
        try {
            ProcessBuilder pb = new ProcessBuilder(
                "bash",
                config.getClusterScriptPath(),
                "restart-broker",
                "--work-dir", config.getWorkDir(),
                "--broker-id", Integer.toString(targetBrokerIdx),
                "--mode", mode
            );
            pb.redirectErrorStream(true);
            Process proc = pb.start();
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append(" | ");
                }
            }
            boolean finished = proc.waitFor(45, TimeUnit.SECONDS);
            int exitCode = finished ? proc.exitValue() : -1;
            if (!finished) {
                proc.destroyForcibly();
            }

            boolean recovered = exitCode == 0 && awaitClusterBrokersOnline(30000L);
            long elapsedMs = System.currentTimeMillis() - startMs;
            String desc = String.format("round=%d broker-%d mode=%s exitCode=%d recoveryMs=%d",
                round, targetBrokerIdx, mode, exitCode, elapsedMs);
            ledger.recordChaosEvent(desc, recovered);
        } catch (Exception e) {
            log.error("Chaos fault injection failed on broker-{}", targetBrokerIdx, e);
            ledger.recordChaosEvent("round=" + round + " broker-" + targetBrokerIdx + " exception=" + e.getMessage(), false);
        }
    }

    private boolean awaitClusterBrokersOnline(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                ClusterInfo clusterInfo = adminExt.examineBrokerClusterInfo();
                if (clusterInfo != null && clusterInfo.getBrokerAddrTable() != null) {
                    Map<String, BrokerData> table = clusterInfo.getBrokerAddrTable();
                    int onlineMasters = 0;
                    for (BrokerData bd : table.values()) {
                        if (bd.getBrokerAddrs() != null && bd.getBrokerAddrs().containsKey(0L)) {
                            onlineMasters++;
                        }
                    }
                    if (onlineMasters >= expectedBrokerCount) {
                        return true;
                    }
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(500L);
        }
        return false;
    }

    public void stop() {
        active.set(false);
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
