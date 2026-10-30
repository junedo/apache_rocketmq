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
package org.apache.rocketmq.test.longrunning.ledger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.longrunning.config.LongRunningConfig;

public class CorrectnessLedger {

    private static final Logger log = LoggerFactory.getLogger(CorrectnessLedger.class);

    public static final String PROP_RUN_ID = "LR_RUN_ID";
    public static final String PROP_MSG_KEY = "LR_MSG_KEY";
    public static final String PROP_WORKLOAD = "LR_WORKLOAD";
    public static final String PROP_SHARD_KEY = "LR_SHARD_KEY";
    public static final String PROP_SEQ_ID = "LR_SEQ_ID";
    public static final String PROP_CRC32 = "LR_CRC32";
    public static final String PROP_SEND_TS = "LR_SEND_TS";
    public static final String PROP_EXPECTED_DELIVER_TS = "LR_EXPECTED_DELIVER_TS";

    public enum TxState {
        NON_TX,
        PENDING,
        COMMITTED,
        ROLLED_BACK,
        FILTERED_OUT
    }

    public static class TrackedMessage {
        private final String msgKey;
        private final String workload;
        private final String shardingKey;
        private final long seqId;
        private final long crc32;
        private final long sendTimestamp;
        private final long expectedDeliverTimestamp;
        private volatile TxState txState;
        private final AtomicBoolean consumed = new AtomicBoolean(false);
        private final AtomicLong consumeCount = new AtomicLong(0);

        public TrackedMessage(String msgKey, String workload, String shardingKey, long seqId,
            long crc32, long sendTimestamp, long expectedDeliverTimestamp, TxState txState) {
            this.msgKey = msgKey;
            this.workload = workload;
            this.shardingKey = shardingKey;
            this.seqId = seqId;
            this.crc32 = crc32;
            this.sendTimestamp = sendTimestamp;
            this.expectedDeliverTimestamp = expectedDeliverTimestamp;
            this.txState = txState;
        }

        public String getMsgKey() {
            return msgKey;
        }

        public String getWorkload() {
            return workload;
        }

        public String getShardingKey() {
            return shardingKey;
        }

        public long getSeqId() {
            return seqId;
        }

        public long getCrc32() {
            return crc32;
        }

        public long getSendTimestamp() {
            return sendTimestamp;
        }

        public long getExpectedDeliverTimestamp() {
            return expectedDeliverTimestamp;
        }

        public TxState getTxState() {
            return txState;
        }

        public void setTxState(TxState txState) {
            this.txState = txState;
        }

        public boolean isExpectedToDeliver() {
            return txState == TxState.NON_TX || txState == TxState.COMMITTED;
        }

        public boolean isConsumed() {
            return consumed.get();
        }
    }

    private final LongRunningConfig config;
    private final ConcurrentMap<String, TrackedMessage> trackedMessages = new ConcurrentHashMap<>(65536);
    private final ConcurrentMap<String, AtomicLong> sentByWorkload = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> consumedByWorkload = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, AtomicLong> fifoMaxConsumedSeq = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<Long>> fifoConsumedSeqSet = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> fifoMaxSentSeq = new ConcurrentHashMap<>();

    private final AtomicLong totalExpectedDelivered = new AtomicLong(0);
    private final AtomicLong totalUniqueConsumed = new AtomicLong(0);
    private final AtomicLong totalDuplicateConsumed = new AtomicLong(0);
    private final AtomicLong totalRolledBack = new AtomicLong(0);
    private final AtomicLong totalFilteredOut = new AtomicLong(0);

    private final AtomicLong crcCorruptionCount = new AtomicLong(0);
    private final AtomicLong fifoOrderViolationCount = new AtomicLong(0);
    private final AtomicLong fifoGapViolationCount = new AtomicLong(0);
    private final AtomicLong rollbackLeakCount = new AtomicLong(0);
    private final AtomicLong filterLeakCount = new AtomicLong(0);
    private final AtomicLong prematureTimerDeliveryCount = new AtomicLong(0);

    private final Map<String, String> e2eCaseResults = Collections.synchronizedMap(new LinkedHashMap<>());
    private final AtomicLong e2eCasesTotal = new AtomicLong(0);
    private final AtomicLong e2eCasesPassed = new AtomicLong(0);
    private final AtomicLong e2eCasesFailed = new AtomicLong(0);

    private final ConcurrentLinkedQueue<Long> latencySamples = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> chaosLogs = new ConcurrentLinkedQueue<>();
    private final AtomicLong chaosTriggeredCount = new AtomicLong(0);
    private final AtomicLong chaosSucceededCount = new AtomicLong(0);

    public CorrectnessLedger(LongRunningConfig config) {
        this.config = config;
    }

    public Message buildTrackedMessage(String topic, String tag, String workload,
        String shardingKey, long seqId, long expectedDeliverTimeMs) {
        String msgKey = config.getRunId() + "-" + workload + "-" + shardingKey + "-" + seqId;
        int size = Math.max(64, config.getPayloadSizeBytes());
        byte[] body = generateDeterministicPayload(msgKey, size);
        long crc = computeCrc32(body);
        long now = System.currentTimeMillis();

        Message msg = new Message(topic, tag, msgKey, body);
        msg.putUserProperty(PROP_RUN_ID, config.getRunId());
        msg.putUserProperty(PROP_MSG_KEY, msgKey);
        msg.putUserProperty(PROP_WORKLOAD, workload);
        msg.putUserProperty(PROP_SHARD_KEY, shardingKey);
        msg.putUserProperty(PROP_SEQ_ID, Long.toString(seqId));
        msg.putUserProperty(PROP_CRC32, Long.toString(crc));
        msg.putUserProperty(PROP_SEND_TS, Long.toString(now));
        msg.putUserProperty(PROP_EXPECTED_DELIVER_TS, Long.toString(expectedDeliverTimeMs));
        return msg;
    }

    public void registerNonTxSent(Message msg) {
        String msgKey = msg.getUserProperty(PROP_MSG_KEY);
        String workload = msg.getUserProperty(PROP_WORKLOAD);
        String shardingKey = msg.getUserProperty(PROP_SHARD_KEY);
        long seqId = Long.parseLong(msg.getUserProperty(PROP_SEQ_ID));
        long crc = Long.parseLong(msg.getUserProperty(PROP_CRC32));
        long sendTs = Long.parseLong(msg.getUserProperty(PROP_SEND_TS));
        long expectedDeliverTs = Long.parseLong(msg.getUserProperty(PROP_EXPECTED_DELIVER_TS));

        TrackedMessage tracked = new TrackedMessage(
            msgKey, workload, shardingKey, seqId, crc, sendTs, expectedDeliverTs, TxState.NON_TX);
        TrackedMessage existing = trackedMessages.putIfAbsent(msgKey, tracked);
        if (existing == null) {
            totalExpectedDelivered.incrementAndGet();
            sentByWorkload.computeIfAbsent(workload, k -> new AtomicLong(0)).incrementAndGet();
            if ("FIFO".equals(workload)) {
                fifoMaxSentSeq.computeIfAbsent(shardingKey, k -> new AtomicLong(-1))
                    .accumulateAndGet(seqId, Math::max);
            }
        }
    }

    public void registerTxPrepared(Message msg) {
        String msgKey = msg.getUserProperty(PROP_MSG_KEY);
        String workload = msg.getUserProperty(PROP_WORKLOAD);
        String shardingKey = msg.getUserProperty(PROP_SHARD_KEY);
        long seqId = Long.parseLong(msg.getUserProperty(PROP_SEQ_ID));
        long crc = Long.parseLong(msg.getUserProperty(PROP_CRC32));
        long sendTs = Long.parseLong(msg.getUserProperty(PROP_SEND_TS));
        long expectedDeliverTs = Long.parseLong(msg.getUserProperty(PROP_EXPECTED_DELIVER_TS));

        TrackedMessage tracked = new TrackedMessage(
            msgKey, workload, shardingKey, seqId, crc, sendTs, expectedDeliverTs, TxState.PENDING);
        trackedMessages.putIfAbsent(msgKey, tracked);
    }

    public void markTxCommitted(String msgKey) {
        TrackedMessage tracked = trackedMessages.get(msgKey);
        if (tracked != null) {
            synchronized (tracked) {
                if (tracked.getTxState() != TxState.COMMITTED) {
                    tracked.setTxState(TxState.COMMITTED);
                    totalExpectedDelivered.incrementAndGet();
                    sentByWorkload.computeIfAbsent(tracked.getWorkload(), k -> new AtomicLong(0)).incrementAndGet();
                }
            }
        }
    }

    public void markTxRolledBack(String msgKey) {
        TrackedMessage tracked = trackedMessages.get(msgKey);
        if (tracked != null) {
            synchronized (tracked) {
                if (tracked.getTxState() != TxState.ROLLED_BACK) {
                    tracked.setTxState(TxState.ROLLED_BACK);
                    totalRolledBack.incrementAndGet();
                }
            }
        }
    }

    public void removeUnsent(String msgKey) {
        TrackedMessage removed = trackedMessages.remove(msgKey);
        if (removed != null) {
            if (removed.isExpectedToDeliver() && !removed.isConsumed()) {
                totalExpectedDelivered.decrementAndGet();
                AtomicLong counter = sentByWorkload.get(removed.getWorkload());
                if (counter != null) {
                    counter.decrementAndGet();
                }
            } else if (removed.getTxState() == TxState.FILTERED_OUT) {
                totalFilteredOut.decrementAndGet();
            }
        }
    }

    public void registerFilteredOutSent(Message msg) {
        String msgKey = msg.getUserProperty(PROP_MSG_KEY);
        String workload = msg.getUserProperty(PROP_WORKLOAD);
        String shardingKey = msg.getUserProperty(PROP_SHARD_KEY);
        long seqId = Long.parseLong(msg.getUserProperty(PROP_SEQ_ID));
        long crc = Long.parseLong(msg.getUserProperty(PROP_CRC32));
        long sendTs = Long.parseLong(msg.getUserProperty(PROP_SEND_TS));
        long expectedDeliverTs = Long.parseLong(msg.getUserProperty(PROP_EXPECTED_DELIVER_TS));

        TrackedMessage tracked = new TrackedMessage(
            msgKey, workload, shardingKey, seqId, crc, sendTs, expectedDeliverTs, TxState.FILTERED_OUT);
        TrackedMessage existing = trackedMessages.putIfAbsent(msgKey, tracked);
        if (existing == null) {
            totalFilteredOut.incrementAndGet();
        }
    }

    public void recordE2ECaseResult(String caseName, boolean passed, long elapsedMs, String detail) {
        e2eCasesTotal.incrementAndGet();
        if (passed) {
            e2eCasesPassed.incrementAndGet();
            e2eCaseResults.put(caseName, String.format("PASS (%d ms)", elapsedMs));
        } else {
            e2eCasesFailed.incrementAndGet();
            e2eCaseResults.put(caseName, String.format("FAIL (%d ms): %s", elapsedMs, detail));
            recordViolation(String.format("[E2E_CASE_FAILED] %s: %s", caseName, detail));
        }
    }

    public void recordConsumed(MessageExt msgExt) {
        if (msgExt == null) {
            return;
        }
        String runId = msgExt.getUserProperty(PROP_RUN_ID);
        if (!config.getRunId().equals(runId)) {
            return;
        }
        String msgKey = msgExt.getUserProperty(PROP_MSG_KEY);
        String workload = msgExt.getUserProperty(PROP_WORKLOAD);
        String shardingKey = msgExt.getUserProperty(PROP_SHARD_KEY);
        String seqStr = msgExt.getUserProperty(PROP_SEQ_ID);
        String crcStr = msgExt.getUserProperty(PROP_CRC32);
        String sendTsStr = msgExt.getUserProperty(PROP_SEND_TS);
        String expectedDeliverStr = msgExt.getUserProperty(PROP_EXPECTED_DELIVER_TS);

        long now = System.currentTimeMillis();
        long actualCrc = computeCrc32(msgExt.getBody());
        long expectedCrc = crcStr != null ? Long.parseLong(crcStr) : -1L;

        // 1. Check CRC32 payload integrity
        if (actualCrc != expectedCrc) {
            crcCorruptionCount.incrementAndGet();
            recordViolation(String.format(
                "[CRC_CORRUPTION] key=%s workload=%s expectedCrc=%d actualCrc=%d",
                msgKey, workload, expectedCrc, actualCrc));
        }

        TrackedMessage tracked = trackedMessages.get(msgKey);
        if (tracked != null && tracked.getTxState() == TxState.ROLLED_BACK) {
            // 2. Check Transaction Isolation (rolled back message must never be delivered)
            rollbackLeakCount.incrementAndGet();
            recordViolation(String.format(
                "[TX_ROLLBACK_LEAK] Rolled back message was delivered to consumer: key=%s", msgKey));
        }
        if (tracked != null && tracked.getTxState() == TxState.FILTERED_OUT) {
            // 2b. Check Tag/SQL Filter Isolation (filtered-out message must never be delivered)
            filterLeakCount.incrementAndGet();
            recordViolation(String.format(
                "[FILTER_LEAK] Filtered-out message was delivered to consumer: key=%s tag=%s",
                msgKey, msgExt.getTags()));
        }

        // 3. Check Timer/Delay delivery accuracy
        if ("DELAY".equals(workload) && expectedDeliverStr != null) {
            long expectedDeliverTs = Long.parseLong(expectedDeliverStr);
            if (expectedDeliverTs > 0 && now + config.getTimerToleranceMs() < expectedDeliverTs) {
                prematureTimerDeliveryCount.incrementAndGet();
                recordViolation(String.format(
                    "[TIMER_PREMATURE] key=%s delivered early by %d ms (now=%d, expected=%d)",
                    msgKey, expectedDeliverTs - now, now, expectedDeliverTs));
            }
        }

        // 4. Check FIFO ordering per shardingKey
        if ("FIFO".equals(workload) && shardingKey != null && seqStr != null) {
            long seqId = Long.parseLong(seqStr);
            fifoConsumedSeqSet.computeIfAbsent(shardingKey, k -> ConcurrentHashMap.newKeySet()).add(seqId);
            AtomicLong lastSeq = fifoMaxConsumedSeq.computeIfAbsent(shardingKey, k -> new AtomicLong(-1L));
            synchronized (lastSeq) {
                long prev = lastSeq.get();
                if (seqId > prev + 1) {
                    fifoOrderViolationCount.incrementAndGet();
                    recordViolation(String.format(
                        "[FIFO_ORDER_GAP] shardingKey=%s jumped from seq=%d to seq=%d",
                        shardingKey, prev, seqId));
                    lastSeq.set(seqId);
                } else if (seqId == prev + 1) {
                    lastSeq.set(seqId);
                }
                // Note: seqId <= prev is an at-least-once redelivery after broker restart, tracked as duplicate below
            }
        }

        // 5. Track unique vs duplicate consumption & latency
        if (tracked != null) {
            long count = tracked.consumeCount.incrementAndGet();
            if (tracked.consumed.compareAndSet(false, true)) {
                totalUniqueConsumed.incrementAndGet();
                consumedByWorkload.computeIfAbsent(workload, k -> new AtomicLong(0)).incrementAndGet();
                if (sendTsStr != null) {
                    long sendTs = Long.parseLong(sendTsStr);
                    long baseTs = "DELAY".equals(workload) && expectedDeliverStr != null
                        ? Math.max(sendTs, Long.parseLong(expectedDeliverStr))
                        : sendTs;
                    long latency = Math.max(0L, now - baseTs);
                    if (latencySamples.size() < 200000) {
                        latencySamples.add(latency);
                    }
                }
            } else if (count > 1) {
                totalDuplicateConsumed.incrementAndGet();
            }
        } else {
            // Message arrived before registerNonTxSent completed on producer thread; create entry and mark consumed
            long seqId = seqStr != null ? Long.parseLong(seqStr) : 0L;
            long sendTs = sendTsStr != null ? Long.parseLong(sendTsStr) : now;
            long expectedDeliverTs = expectedDeliverStr != null ? Long.parseLong(expectedDeliverStr) : 0L;
            TrackedMessage early = new TrackedMessage(
                msgKey, workload, shardingKey, seqId, expectedCrc, sendTs, expectedDeliverTs,
                "TRANSACTION".equals(workload) ? TxState.COMMITTED : TxState.NON_TX);
            early.consumed.set(true);
            early.consumeCount.set(1);
            TrackedMessage prev = trackedMessages.putIfAbsent(msgKey, early);
            if (prev == null) {
                totalExpectedDelivered.incrementAndGet();
                sentByWorkload.computeIfAbsent(workload, k -> new AtomicLong(0)).incrementAndGet();
                totalUniqueConsumed.incrementAndGet();
                consumedByWorkload.computeIfAbsent(workload, k -> new AtomicLong(0)).incrementAndGet();
            } else if (prev.consumed.compareAndSet(false, true)) {
                totalUniqueConsumed.incrementAndGet();
                consumedByWorkload.computeIfAbsent(workload, k -> new AtomicLong(0)).incrementAndGet();
            } else {
                totalDuplicateConsumed.incrementAndGet();
            }
        }
    }

    public void recordChaosEvent(String description, boolean success) {
        chaosTriggeredCount.incrementAndGet();
        if (success) {
            chaosSucceededCount.incrementAndGet();
        } else {
            recordViolation("[CHAOS_RECOVERY_FAILURE] " + description);
        }
        String entry = String.format("[%d] success=%s %s", System.currentTimeMillis(), success, description);
        chaosLogs.add(entry);
        log.info("Chaos event recorded: {}", entry);
    }

    public long getPendingCount() {
        long pending = 0L;
        for (TrackedMessage msg : trackedMessages.values()) {
            if (msg.isExpectedToDeliver() && !msg.isConsumed()) {
                pending++;
            }
        }
        return pending;
    }

    public boolean awaitDrain(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            long pending = getPendingCount();
            if (pending == 0L) {
                return true;
            }
            log.info("Waiting for message drain: pending={}, expected={}, uniqueConsumed={}",
                pending, totalExpectedDelivered.get(), totalUniqueConsumed.get());
            Thread.sleep(500L);
        }
        return getPendingCount() == 0L;
    }

    public VerificationReport buildReport(long durationMs) {
        VerificationReport report = new VerificationReport();
        report.setRunId(config.getRunId());
        report.setProfile(config.getProfile().getName());
        report.setDurationMs(durationMs);

        // Verify FIFO contiguous sequence completeness
        for (Map.Entry<String, AtomicLong> entry : fifoMaxSentSeq.entrySet()) {
            String shardKey = entry.getKey();
            long maxSeq = entry.getValue().get();
            Set<Long> consumedSet = fifoConsumedSeqSet.getOrDefault(shardKey, Collections.emptySet());
            for (long s = 0; s <= maxSeq; s++) {
                if (!consumedSet.contains(s)) {
                    fifoGapViolationCount.incrementAndGet();
                    recordViolation(String.format("[FIFO_MISSING_SEQ] shardingKey=%s missing seq=%d (max=%d)",
                        shardKey, s, maxSeq));
                }
            }
        }

        // Count any unconsumed messages
        long unconsumed = 0L;
        for (TrackedMessage msg : trackedMessages.values()) {
            if (msg.isExpectedToDeliver() && !msg.isConsumed()) {
                unconsumed++;
                if (unconsumed <= 20) {
                    recordViolation(String.format("[MESSAGE_LOST] key=%s workload=%s seq=%d",
                        msg.getMsgKey(), msg.getWorkload(), msg.getSeqId()));
                }
            }
        }

        report.setTotalExpectedDelivered(totalExpectedDelivered.get());
        report.setTotalUniqueConsumed(totalUniqueConsumed.get());
        report.setTotalDuplicateConsumed(totalDuplicateConsumed.get());
        report.setTotalRolledBack(totalRolledBack.get());
        report.setTotalFilteredOut(totalFilteredOut.get());
        report.setUnconsumedCount(unconsumed);

        report.setCrcCorruptionCount(crcCorruptionCount.get());
        report.setFifoOrderViolationCount(fifoOrderViolationCount.get());
        report.setFifoGapViolationCount(fifoGapViolationCount.get());
        report.setRollbackLeakCount(rollbackLeakCount.get());
        report.setFilterLeakCount(filterLeakCount.get());
        report.setPrematureTimerDeliveryCount(prematureTimerDeliveryCount.get());

        report.setE2eCasesTotal((int) e2eCasesTotal.get());
        report.setE2eCasesPassed((int) e2eCasesPassed.get());
        report.setE2eCasesFailed((int) e2eCasesFailed.get());
        report.setE2eCaseResults(new LinkedHashMap<>(e2eCaseResults));

        report.setChaosEventsTriggered((int) chaosTriggeredCount.get());
        report.setChaosEventsSucceeded((int) chaosSucceededCount.get());

        Map<String, Long> sentMap = new LinkedHashMap<>();
        for (Map.Entry<String, AtomicLong> e : sentByWorkload.entrySet()) {
            sentMap.put(e.getKey(), e.getValue().get());
        }
        report.setSentByWorkload(sentMap);

        Map<String, Long> consumedMap = new LinkedHashMap<>();
        for (Map.Entry<String, AtomicLong> e : consumedByWorkload.entrySet()) {
            consumedMap.put(e.getKey(), e.getValue().get());
        }
        report.setConsumedByWorkload(consumedMap);

        List<Long> sortedLatencies = new ArrayList<>(latencySamples);
        Collections.sort(sortedLatencies);
        if (!sortedLatencies.isEmpty()) {
            report.setP50LatencyMs(percentile(sortedLatencies, 0.50));
            report.setP95LatencyMs(percentile(sortedLatencies, 0.95));
            report.setP99LatencyMs(percentile(sortedLatencies, 0.99));
            report.setMaxLatencyMs(sortedLatencies.get(sortedLatencies.size() - 1));
        }

        report.setViolations(new ArrayList<>(violations));
        report.setChaosLogs(new ArrayList<>(chaosLogs));

        boolean allPassed = (totalExpectedDelivered.get() > 0 || e2eCasesTotal.get() > 0)
            && unconsumed == 0
            && crcCorruptionCount.get() == 0
            && fifoOrderViolationCount.get() == 0
            && fifoGapViolationCount.get() == 0
            && rollbackLeakCount.get() == 0
            && filterLeakCount.get() == 0
            && prematureTimerDeliveryCount.get() == 0
            && e2eCasesFailed.get() == 0
            && chaosTriggeredCount.get() == chaosSucceededCount.get();
        report.setPassed(allPassed);
        return report;
    }

    private void recordViolation(String msg) {
        log.error("Invariant violation detected: {}", msg);
        if (violations.size() < 100) {
            violations.add(msg);
        }
    }

    private static long percentile(List<Long> sorted, double p) {
        int idx = (int) Math.min(sorted.size() - 1, Math.floor(sorted.size() * p));
        return sorted.get(Math.max(0, idx));
    }

    private static byte[] generateDeterministicPayload(String headerKey, int size) {
        byte[] prefix = (headerKey + "|").getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[Math.max(size, prefix.length + 16)];
        System.arraycopy(prefix, 0, out, 0, prefix.length);
        for (int i = prefix.length; i < out.length; i++) {
            out[i] = (byte) ('A' + ThreadLocalRandom.current().nextInt(26));
        }
        return out;
    }

    public static long computeCrc32(byte[] data) {
        if (data == null) {
            return 0L;
        }
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return crc32.getValue();
    }
}
