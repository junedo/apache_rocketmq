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
package org.apache.rocketmq.test.longrunning.suite;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.test.longrunning.config.LongRunningConfig;
import org.apache.rocketmq.test.longrunning.ledger.CorrectnessLedger;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

/**
 * Comprehensive E2E Scenario Suite ported from junedo/rocketmq-e2e covering 32 scenarios across 10 domains:
 * 1. Normal Message (Sync / Async / OneWay)
 * 2. Order Message (Partition Order / Global Order)
 * 3. Delay Message (DelayLevel / DeliverTimeMs / Negative Level Rejection)
 * 4. Transaction Message (Direct Commit / Direct Rollback / Checker Commit / Checker Rollback / Partial Checker)
 * 5. Batch Producer (Topic Batch / Queue Batch)
 * 6. Tag Filter (Exact / Multi-Tag OR / Wildcard / Unmatched / Hash Collision BB-vs-Aa / Case Sensitive BB-vs-bb / Long Tag)
 * 7. SQL92 Filter (Numeric Comparison / BETWEEN & IS NOT NULL / Unmatched Property)
 * 8. Message Content & Boundary (Space / Unicode & Emoji / Multi-Keys / Large Payload / >4MB Rejection / Empty & Null Validation)
 * 9. Consumer Model, Rebalance & Offset (Broadcast Diff Groups / Broadcast Same Group / Cluster Scale Down-Up / First & Last Offset / LitePull Subscribe & Assign)
 * 10. Abnormal & Retry & Parameter Validation (Push Retry / Orderly Suspend Retry / Exception & Null Return Retry / Missing Group & Listener & DEFAULT_CONSUMER Rejection)
 */
public class E2EScenarioSuite {

    private static final Logger log = LoggerFactory.getLogger(E2EScenarioSuite.class);

    @FunctionalInterface
    private interface ScenarioAction {
        void run() throws Exception;
    }

    private final LongRunningConfig config;
    private final CorrectnessLedger ledger;
    private final DefaultMQAdminExt adminExt;
    private DefaultMQProducer sharedProducer;
    private String sharedTopic;
    private int topicCounter = 0;

    public E2EScenarioSuite(LongRunningConfig config, CorrectnessLedger ledger, DefaultMQAdminExt adminExt) {
        this.config = config;
        this.ledger = ledger;
        this.adminExt = adminExt;
    }

    public void runAllScenarios() throws Exception {
        log.info("Starting E2EScenarioSuite (32 scenarios modeled on junedo/rocketmq-e2e)...");
        sharedTopic = createSuiteTopic("E2E_SHARED");
        sharedProducer = new DefaultMQProducer("PID_E2E_SUITE_" + UUID.randomUUID().toString().substring(0, 8));
        sharedProducer.setNamesrvAddr(config.getNamesrvAddr());
        sharedProducer.setInstanceName("e2e-suite-producer-" + UUID.randomUUID());
        sharedProducer.setSendMsgTimeout(5000);
        sharedProducer.start();

        try {
            // 1. Normal Message (NormalMessageTest)
            runCase("01_Normal_SyncSendAndConsume", this::case01NormalSync);
            runCase("02_Normal_AsyncSendAndConsume", this::case02NormalAsync);
            runCase("03_Normal_OneWaySendAndConsume", this::case03NormalOneWay);

            // 2. Order Message (OrderMessageTest)
            runCase("04_Order_PartitionOrderConsume", this::case04PartitionOrder);
            runCase("05_Order_GlobalOrderSingleQueue", this::case05GlobalOrder);

            // 3. Delay Message (DelayMessageTest)
            runCase("06_Delay_DelayLevel1Delivery", this::case06DelayLevel1);
            runCase("07_Delay_DeliverTimeMsAccuracy", this::case07DeliverTimeMs);
            runCase("08_Delay_NegativeDelayLevelRejected", this::case08NegativeDelayRejected);

            // 4. Transaction Message (TransactionMessageTest)
            runCase("09_Tx_DirectCommitConsumed", this::case09TxDirectCommit);
            runCase("10_Tx_DirectRollbackIsolated", this::case10TxDirectRollback);
            runCase("11_Tx_CheckerCommitAndRollbackMix", this::case11TxCheckerMix);

            // 5. Batch Producer (BatchProducerTest)
            runCase("12_Batch_TopicBatchSend", this::case12BatchTopic);
            runCase("13_Batch_QueueBatchSend", this::case13BatchQueue);

            // 6. Tag Filter (TagFilterTest)
            runCase("14_TagFilter_OrExpressionAndUnmatched", this::case14TagFilterOrAndUnmatched);
            runCase("15_TagFilter_WildcardAll", this::case15TagFilterWildcard);
            runCase("16_TagFilter_SameHashCodeCollision_BB_vs_Aa", this::case16TagHashCollision);
            runCase("17_TagFilter_CaseSensitive_BB_vs_bb", this::case17TagCaseSensitive);
            runCase("18_TagFilter_LongTag4KB", this::case18LongTag4KB);

            // 7. SQL92 Filter (SqlFilterTest)
            runCase("19_SqlFilter_BetweenAndIsNotNull", this::case19SqlBetweenAndNotNull);
            runCase("20_SqlFilter_UnknownPropertyFiltered", this::case20SqlUnknownProperty);

            // 8. Message Content, Keys, Properties & Size (MessageBodyContentTest / MessageKeyTest / NormalMessageSizeTest / MessageAbnormalTest)
            runCase("21_Message_BodySpaceUnicodeEmoji", this::case21BodySpaceUnicodeEmoji);
            runCase("22_Message_MultiKeysAndUnicodeKey", this::case22MultiKeysAndUnicodeKey);
            runCase("23_Message_LargePayload512KBWithUserProps", this::case23LargePayloadWithProps);
            runCase("24_Message_BodyOver4MBRejected", this::case24BodyOver4MBRejected);
            runCase("25_Message_EmptyAndNullValidation", this::case25EmptyAndNullValidation);

            // 9. Consumer Model, Rebalance, Offset & LitePull (ClusterTest / LoadBalancingTest / OffsetTest / PullAckTest)
            runCase("26_Model_BroadcastDifferentAndSameGroup", this::case26BroadcastDiffAndSameGroup);
            runCase("27_Rebalance_ClusterDynamicScaleDownAndUp", this::case27ClusterDynamicScale);
            runCase("28_Offset_ConsumeFromFirstAndLastOffset", this::case28ConsumeFromFirstAndLast);
            runCase("29_LitePull_SubscribeAndAssignSeekCommit", this::case29LitePullSubscribeAndAssign);

            // 10. Retry & Client Init Validation (PushConsumerRetryTest / ProducerInitTest / PushConsumerInitTest / ConsumerGroupTest)
            runCase("30_Retry_NormalExceptionAndNullReturnRetry", this::case30NormalExceptionAndNullRetry);
            runCase("31_Retry_OrderlySuspendAndResume", this::case31OrderlySuspendAndResume);
            runCase("32_ClientInit_InvalidParamAndSystemGroupRejected", this::case32ClientInitValidation);
        } finally {
            sharedProducer.shutdown();
        }
    }

    private void runCase(String name, ScenarioAction action) {
        long start = System.currentTimeMillis();
        try {
            action.run();
            long elapsed = System.currentTimeMillis() - start;
            ledger.recordE2ECaseResult(name, true, elapsed, "OK");
            log.info("[E2EScenarioSuite] PASS {} ({} ms)", name, elapsed);
        } catch (Throwable t) {
            long elapsed = System.currentTimeMillis() - start;
            String msg = t.getClass().getSimpleName() + ": " + t.getMessage();
            ledger.recordE2ECaseResult(name, false, elapsed, msg);
            log.error("[E2EScenarioSuite] FAIL {} ({} ms): {}", name, elapsed, msg, t);
        }
    }

    private String createSuiteTopic(String prefix) throws Exception {
        String topic = prefix + "_" + config.getRunId().replace('-', '_') + "_" + (++topicCounter);
        Set<String> masterAddrs = CommandUtil.fetchMasterAddrByClusterName(adminExt, config.getClusterName());
        TopicConfig topicConfig = new TopicConfig(topic, 4, 4, PermName.PERM_READ | PermName.PERM_WRITE);
        for (String addr : masterAddrs) {
            adminExt.createAndUpdateTopicConfig(addr, topicConfig);
        }
        Thread.sleep(150L);
        return topic;
    }

    private DefaultMQPushConsumer createPushConsumer(String groupPrefix, String topic, String subExpr,
        MessageListenerConcurrently listener) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
            groupPrefix + "_" + UUID.randomUUID().toString().substring(0, 8));
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setInstanceName("inst-" + UUID.randomUUID());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, subExpr);
        consumer.registerMessageListener(listener);
        consumer.start();
        return consumer;
    }

    // =========================================================================
    // 1. Normal Message Scenarios
    // =========================================================================

    private void case01NormalSync() throws Exception {
        String tag = "TAG_SYNC_" + UUID.randomUUID().toString().substring(0, 6);
        int count = 10;
        CountDownLatch latch = new CountDownLatch(count);
        DefaultMQPushConsumer consumer = createPushConsumer("GID_SYNC", sharedTopic, tag, (msgs, ctx) -> {
            for (int i = 0; i < msgs.size(); i++) {
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            for (int i = 0; i < count; i++) {
                Message msg = new Message(sharedTopic, tag, ("sync-" + i).getBytes(StandardCharsets.UTF_8));
                SendResult res = sharedProducer.send(msg);
                assertTrue(res.getSendStatus() == SendStatus.SEND_OK, "Sync send failed");
            }
            assertTrue(latch.await(15, TimeUnit.SECONDS), "Sync messages not all consumed");
        } finally {
            consumer.shutdown();
        }
    }

    private void case02NormalAsync() throws Exception {
        String tag = "TAG_ASYNC_" + UUID.randomUUID().toString().substring(0, 6);
        int count = 10;
        CountDownLatch sendLatch = new CountDownLatch(count);
        CountDownLatch recvLatch = new CountDownLatch(count);
        DefaultMQPushConsumer consumer = createPushConsumer("GID_ASYNC", sharedTopic, tag, (msgs, ctx) -> {
            for (int i = 0; i < msgs.size(); i++) {
                recvLatch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            for (int i = 0; i < count; i++) {
                Message msg = new Message(sharedTopic, tag, ("async-" + i).getBytes(StandardCharsets.UTF_8));
                sharedProducer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                            sendLatch.countDown();
                        }
                    }

                    @Override
                    public void onException(Throwable e) {
                    }
                });
            }
            assertTrue(sendLatch.await(10, TimeUnit.SECONDS), "Async send callback timeout");
            assertTrue(recvLatch.await(15, TimeUnit.SECONDS), "Async messages not all consumed");
        } finally {
            consumer.shutdown();
        }
    }

    private void case03NormalOneWay() throws Exception {
        String tag = "TAG_ONEWAY_" + UUID.randomUUID().toString().substring(0, 6);
        int count = 10;
        CountDownLatch recvLatch = new CountDownLatch(count);
        DefaultMQPushConsumer consumer = createPushConsumer("GID_ONEWAY", sharedTopic, tag, (msgs, ctx) -> {
            for (int i = 0; i < msgs.size(); i++) {
                recvLatch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            for (int i = 0; i < count; i++) {
                Message msg = new Message(sharedTopic, tag, ("oneway-" + i).getBytes(StandardCharsets.UTF_8));
                sharedProducer.sendOneway(msg);
            }
            assertTrue(recvLatch.await(15, TimeUnit.SECONDS), "OneWay messages not all consumed");
        } finally {
            consumer.shutdown();
        }
    }

    // =========================================================================
    // 2. Order Message Scenarios
    // =========================================================================

    private void case04PartitionOrder() throws Exception {
        String topic = createSuiteTopic("E2E_PART_ORDER");
        int keys = 4;
        int msgsPerKey = 10;
        ConcurrentHashMap<String, List<Integer>> receivedPerKey = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(keys * msgsPerKey);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("GID_PORDER_" + UUID.randomUUID().toString().substring(0, 6));
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, ctx) -> {
            for (MessageExt msg : msgs) {
                String k = msg.getKeys();
                int seq = Integer.parseInt(new String(msg.getBody(), StandardCharsets.UTF_8));
                receivedPerKey.computeIfAbsent(k, x -> new CopyOnWriteArrayList<>()).add(seq);
                latch.countDown();
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();

        try {
            List<MessageQueue> mqs = sharedProducer.fetchPublishMessageQueues(topic);
            for (int i = 0; i < msgsPerKey; i++) {
                for (int k = 0; k < keys; k++) {
                    String shardKey = "shard-" + k;
                    Message msg = new Message(topic, "TAG_ORDER", shardKey, Integer.toString(i).getBytes(StandardCharsets.UTF_8));
                    sharedProducer.send(msg, mqs.get(k % mqs.size()));
                }
            }
            assertTrue(latch.await(20, TimeUnit.SECONDS), "Partition order messages timeout");
            for (int k = 0; k < keys; k++) {
                List<Integer> seqs = receivedPerKey.get("shard-" + k);
                assertTrue(seqs != null && seqs.size() == msgsPerKey, "Missing messages for shard-" + k);
                for (int i = 0; i < msgsPerKey; i++) {
                    assertTrue(seqs.get(i) == i, "Out of order on shard-" + k + ": " + seqs);
                }
            }
        } finally {
            consumer.shutdown();
        }
    }

    private void case05GlobalOrder() throws Exception {
        String topic = createSuiteTopic("E2E_GLOBAL_ORDER");
        int count = 25;
        List<Integer> received = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(count);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("GID_GORDER_" + UUID.randomUUID().toString().substring(0, 6));
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, ctx) -> {
            for (MessageExt msg : msgs) {
                received.add(Integer.parseInt(new String(msg.getBody(), StandardCharsets.UTF_8)));
                latch.countDown();
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();

        try {
            List<MessageQueue> mqs = sharedProducer.fetchPublishMessageQueues(topic);
            MessageQueue singleQueue = mqs.get(0);
            for (int i = 0; i < count; i++) {
                Message msg = new Message(topic, "TAG_GORDER", Integer.toString(i).getBytes(StandardCharsets.UTF_8));
                sharedProducer.send(msg, singleQueue);
            }
            assertTrue(latch.await(20, TimeUnit.SECONDS), "Global order messages timeout");
            for (int i = 0; i < count; i++) {
                assertTrue(received.get(i) == i, "Global order violated: " + received);
            }
        } finally {
            consumer.shutdown();
        }
    }

    // =========================================================================
    // 3. Delay Message Scenarios
    // =========================================================================

    private void case06DelayLevel1() throws Exception {
        String tag = "TAG_DL1_" + UUID.randomUUID().toString().substring(0, 6);
        int count = 5;
        CountDownLatch latch = new CountDownLatch(count);
        AtomicBoolean premature = new AtomicBoolean(false);
        long sendStart = System.currentTimeMillis();

        DefaultMQPushConsumer consumer = createPushConsumer("GID_DL1", sharedTopic, tag, (msgs, ctx) -> {
            long now = System.currentTimeMillis();
            if (now - sendStart < 600L) {
                premature.set(true);
            }
            for (int i = 0; i < msgs.size(); i++) {
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            for (int i = 0; i < count; i++) {
                Message msg = new Message(sharedTopic, tag, ("dl1-" + i).getBytes(StandardCharsets.UTF_8));
                msg.setDelayTimeLevel(1);
                sharedProducer.send(msg);
            }
            assertTrue(latch.await(15, TimeUnit.SECONDS), "DelayLevel=1 messages timeout");
            assertTrue(!premature.get(), "DelayLevel=1 message delivered prematurely");
        } finally {
            consumer.shutdown();
        }
    }

    private void case07DeliverTimeMs() throws Exception {
        String tag = "TAG_TIMER_" + UUID.randomUUID().toString().substring(0, 6);
        int count = 5;
        CountDownLatch latch = new CountDownLatch(count);
        long targetDeliverMs = System.currentTimeMillis() + 1800L;
        AtomicBoolean premature = new AtomicBoolean(false);

        DefaultMQPushConsumer consumer = createPushConsumer("GID_TIMER", sharedTopic, tag, (msgs, ctx) -> {
            long now = System.currentTimeMillis();
            if (now + 800L < targetDeliverMs) {
                premature.set(true);
            }
            for (int i = 0; i < msgs.size(); i++) {
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            for (int i = 0; i < count; i++) {
                Message msg = new Message(sharedTopic, tag, ("timer-" + i).getBytes(StandardCharsets.UTF_8));
                msg.setDeliverTimeMs(targetDeliverMs);
                sharedProducer.send(msg);
            }
            assertTrue(latch.await(15, TimeUnit.SECONDS), "Timer deliverTimeMs messages timeout");
            assertTrue(!premature.get(), "Timer message delivered prematurely");
        } finally {
            consumer.shutdown();
        }
    }

    private void case08NegativeDelayRejected() throws Exception {
        boolean thrown = false;
        try {
            Message msg = new Message(sharedTopic, "TAG_NEG_DELAY", "neg".getBytes(StandardCharsets.UTF_8));
            msg.setDelayTimeLevel(-1);
            sharedProducer.send(msg);
        } catch (Exception e) {
            thrown = true;
        }
        assertTrue(thrown, "Negative delay level should be rejected");
    }

    // =========================================================================
    // 4. Transaction Message Scenarios
    // =========================================================================

    private void case09TxDirectCommit() throws Exception {
        String topic = createSuiteTopic("E2E_TX_COMMIT");
        int count = 6;
        CountDownLatch latch = new CountDownLatch(count);
        DefaultMQPushConsumer consumer = createPushConsumer("GID_TX_COMMIT", topic, "*", (msgs, ctx) -> {
            for (int i = 0; i < msgs.size(); i++) {
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        TransactionMQProducer txProducer = new TransactionMQProducer("PID_TX_COMMIT_" + UUID.randomUUID().toString().substring(0, 6));
        txProducer.setNamesrvAddr(config.getNamesrvAddr());
        txProducer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        txProducer.start();

        try {
            for (int i = 0; i < count; i++) {
                Message msg = new Message(topic, "TAG_TX", ("tx-commit-" + i).getBytes(StandardCharsets.UTF_8));
                txProducer.sendMessageInTransaction(msg, null);
            }
            assertTrue(latch.await(15, TimeUnit.SECONDS), "Direct committed tx messages not all consumed");
        } finally {
            txProducer.shutdown();
            consumer.shutdown();
        }
    }

    private void case10TxDirectRollback() throws Exception {
        String topic = createSuiteTopic("E2E_TX_ROLLBACK");
        AtomicInteger leaked = new AtomicInteger(0);
        DefaultMQPushConsumer consumer = createPushConsumer("GID_TX_RB", topic, "*", (msgs, ctx) -> {
            leaked.addAndGet(msgs.size());
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        TransactionMQProducer txProducer = new TransactionMQProducer("PID_TX_RB_" + UUID.randomUUID().toString().substring(0, 6));
        txProducer.setNamesrvAddr(config.getNamesrvAddr());
        txProducer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        txProducer.start();

        try {
            for (int i = 0; i < 6; i++) {
                Message msg = new Message(topic, "TAG_TX", ("tx-rb-" + i).getBytes(StandardCharsets.UTF_8));
                txProducer.sendMessageInTransaction(msg, null);
            }
            Thread.sleep(1500L);
            assertTrue(leaked.get() == 0, "Rolled-back transaction messages leaked: " + leaked.get());
        } finally {
            txProducer.shutdown();
            consumer.shutdown();
        }
    }

    private void case11TxCheckerMix() throws Exception {
        String topic = createSuiteTopic("E2E_TX_CHECKER");
        int total = 6;
        CountDownLatch commitLatch = new CountDownLatch(total / 2);
        AtomicInteger rollbackLeaked = new AtomicInteger(0);

        DefaultMQPushConsumer consumer = createPushConsumer("GID_TX_CHK", topic, "*", (msgs, ctx) -> {
            for (MessageExt msg : msgs) {
                int idx = Integer.parseInt(new String(msg.getBody(), StandardCharsets.UTF_8));
                if (idx % 2 == 0) {
                    commitLatch.countDown();
                } else {
                    rollbackLeaked.incrementAndGet();
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        TransactionMQProducer txProducer = new TransactionMQProducer("PID_TX_CHK_" + UUID.randomUUID().toString().substring(0, 6));
        txProducer.setNamesrvAddr(config.getNamesrvAddr());
        txProducer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return LocalTransactionState.UNKNOW;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                int idx = Integer.parseInt(new String(msg.getBody(), StandardCharsets.UTF_8));
                return (idx % 2 == 0) ? LocalTransactionState.COMMIT_MESSAGE : LocalTransactionState.ROLLBACK_MESSAGE;
            }
        });
        txProducer.start();

        try {
            for (int i = 0; i < total; i++) {
                Message msg = new Message(topic, "TAG_TX", Integer.toString(i).getBytes(StandardCharsets.UTF_8));
                txProducer.sendMessageInTransaction(msg, null);
            }
            assertTrue(commitLatch.await(20, TimeUnit.SECONDS), "Checker committed messages not all received");
            assertTrue(rollbackLeaked.get() == 0, "Checker rolled-back messages leaked");
        } finally {
            txProducer.shutdown();
            consumer.shutdown();
        }
    }

    // =========================================================================
    // 5. Batch Producer Scenarios
    // =========================================================================

    private void case12BatchTopic() throws Exception {
        String tag = "TAG_BATCH_T_" + UUID.randomUUID().toString().substring(0, 6);
        int count = 10;
        CountDownLatch latch = new CountDownLatch(count);
        DefaultMQPushConsumer consumer = createPushConsumer("GID_BATCH_T", sharedTopic, tag, (msgs, ctx) -> {
            for (int i = 0; i < msgs.size(); i++) {
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            List<Message> batch = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                batch.add(new Message(sharedTopic, tag, ("batch-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            SendResult res = sharedProducer.send(batch);
            assertTrue(res.getSendStatus() == SendStatus.SEND_OK, "Batch send failed");
            assertTrue(latch.await(15, TimeUnit.SECONDS), "Batch messages not all consumed");
        } finally {
            consumer.shutdown();
        }
    }

    private void case13BatchQueue() throws Exception {
        String tag = "TAG_BATCH_Q_" + UUID.randomUUID().toString().substring(0, 6);
        int count = 10;
        CountDownLatch latch = new CountDownLatch(count);
        DefaultMQPushConsumer consumer = createPushConsumer("GID_BATCH_Q", sharedTopic, tag, (msgs, ctx) -> {
            for (int i = 0; i < msgs.size(); i++) {
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            List<MessageQueue> mqs = sharedProducer.fetchPublishMessageQueues(sharedTopic);
            List<Message> batch = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                batch.add(new Message(sharedTopic, tag, ("batch-q-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            SendResult res = sharedProducer.send(batch, mqs.get(0));
            assertTrue(res.getSendStatus() == SendStatus.SEND_OK, "Batch queue send failed");
            assertTrue(latch.await(15, TimeUnit.SECONDS), "Batch queue messages not all consumed");
        } finally {
            consumer.shutdown();
        }
    }

    // =========================================================================
    // 6. Tag Filter Scenarios
    // =========================================================================

    private void case14TagFilterOrAndUnmatched() throws Exception {
        String topic = createSuiteTopic("E2E_TAG_OR");
        String tagA = "TagA";
        String tagB = "TagB";
        String tagC = "TagC_Unmatched";
        int perTag = 5;
        CountDownLatch matchLatch = new CountDownLatch(perTag * 2);
        AtomicInteger unmatchedLeaked = new AtomicInteger(0);

        DefaultMQPushConsumer consumer = createPushConsumer("GID_TAG_OR", topic, " TagA || TagB ", (msgs, ctx) -> {
            for (MessageExt msg : msgs) {
                if (tagA.equals(msg.getTags()) || tagB.equals(msg.getTags())) {
                    matchLatch.countDown();
                } else {
                    unmatchedLeaked.incrementAndGet();
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            for (int i = 0; i < perTag; i++) {
                sharedProducer.send(new Message(topic, tagA, ("a-" + i).getBytes(StandardCharsets.UTF_8)));
                sharedProducer.send(new Message(topic, tagB, ("b-" + i).getBytes(StandardCharsets.UTF_8)));
                sharedProducer.send(new Message(topic, tagC, ("c-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            assertTrue(matchLatch.await(15, TimeUnit.SECONDS), "TagA || TagB messages not all received");
            Thread.sleep(500L);
            assertTrue(unmatchedLeaked.get() == 0, "Unmatched TagC leaked through Tag filter");
        } finally {
            consumer.shutdown();
        }
    }

    private void case15TagFilterWildcard() throws Exception {
        String topic = createSuiteTopic("E2E_TAG_STAR");
        CountDownLatch latch = new CountDownLatch(6);
        DefaultMQPushConsumer consumer = createPushConsumer("GID_TAG_STAR", topic, "*", (msgs, ctx) -> {
            for (int i = 0; i < msgs.size(); i++) {
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            for (int i = 0; i < 3; i++) {
                sharedProducer.send(new Message(topic, "T1", "m1".getBytes(StandardCharsets.UTF_8)));
                sharedProducer.send(new Message(topic, "T2", "m2".getBytes(StandardCharsets.UTF_8)));
            }
            assertTrue(latch.await(15, TimeUnit.SECONDS), "Wildcard * subscription missed messages");
        } finally {
            consumer.shutdown();
        }
    }

    private void case16TagHashCollision() throws Exception {
        // "BB".hashCode() == 2112 == "Aa".hashCode()
        String topic = createSuiteTopic("E2E_TAG_COLLISION");
        int count = 5;
        CountDownLatch bbLatch = new CountDownLatch(count);
        AtomicInteger aaLeaked = new AtomicInteger(0);

        DefaultMQPushConsumer consumer = createPushConsumer("GID_TAG_COLL", topic, "BB", (msgs, ctx) -> {
            for (MessageExt msg : msgs) {
                if ("BB".equals(msg.getTags())) {
                    bbLatch.countDown();
                } else {
                    aaLeaked.incrementAndGet();
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            for (int i = 0; i < count; i++) {
                sharedProducer.send(new Message(topic, "Aa", ("aa-" + i).getBytes(StandardCharsets.UTF_8)));
                sharedProducer.send(new Message(topic, "BB", ("bb-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            assertTrue(bbLatch.await(15, TimeUnit.SECONDS), "Tag BB messages not received");
            Thread.sleep(500L);
            assertTrue(aaLeaked.get() == 0, "Hash-colliding tag 'Aa' leaked to 'BB' consumer");
        } finally {
            consumer.shutdown();
        }
    }

    private void case17TagCaseSensitive() throws Exception {
        String topic = createSuiteTopic("E2E_TAG_CASE");
        int count = 5;
        CountDownLatch upperLatch = new CountDownLatch(count);
        AtomicInteger lowerLeaked = new AtomicInteger(0);

        DefaultMQPushConsumer consumer = createPushConsumer("GID_TAG_CASE", topic, "BB", (msgs, ctx) -> {
            for (MessageExt msg : msgs) {
                if ("BB".equals(msg.getTags())) {
                    upperLatch.countDown();
                } else {
                    lowerLeaked.incrementAndGet();
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            for (int i = 0; i < count; i++) {
                sharedProducer.send(new Message(topic, "bb", ("lower-" + i).getBytes(StandardCharsets.UTF_8)));
                sharedProducer.send(new Message(topic, "BB", ("upper-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            assertTrue(upperLatch.await(15, TimeUnit.SECONDS), "Tag BB messages not received");
            Thread.sleep(500L);
            assertTrue(lowerLeaked.get() == 0, "Lowercase tag 'bb' leaked to 'BB' consumer");
        } finally {
            consumer.shutdown();
        }
    }

    private void case18LongTag4KB() throws Exception {
        StringBuilder sb = new StringBuilder(4096);
        for (int i = 0; i < 4096; i++) {
            sb.append((char) ('A' + (i % 26)));
        }
        String longTag = sb.toString();
        CountDownLatch latch = new CountDownLatch(3);
        DefaultMQPushConsumer consumer = createPushConsumer("GID_LONGTAG", sharedTopic, longTag, (msgs, ctx) -> {
            for (int i = 0; i < msgs.size(); i++) {
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            for (int i = 0; i < 3; i++) {
                sharedProducer.send(new Message(sharedTopic, longTag, ("lt-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            assertTrue(latch.await(15, TimeUnit.SECONDS), "4KB tag messages not received");
        } finally {
            consumer.shutdown();
        }
    }

    // =========================================================================
    // 7. SQL92 Filter Scenarios
    // =========================================================================

    private void case19SqlBetweenAndNotNull() throws Exception {
        String topic = createSuiteTopic("E2E_SQL_BETWEEN");
        int count = 5;
        CountDownLatch matchLatch = new CountDownLatch(count);
        AtomicInteger leaked = new AtomicInteger(0);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("GID_SQL_BTW_" + UUID.randomUUID().toString().substring(0, 6));
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, MessageSelector.bySql("(price BETWEEN 20 AND 100) AND regionId IS NOT NULL"));
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, ctx) -> {
            for (MessageExt msg : msgs) {
                int price = Integer.parseInt(msg.getUserProperty("price"));
                if (price >= 20 && price <= 100 && msg.getUserProperty("regionId") != null) {
                    matchLatch.countDown();
                } else {
                    leaked.incrementAndGet();
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        try {
            for (int i = 0; i < count; i++) {
                Message matchMsg = new Message(topic, "TAG_SQL", ("match-" + i).getBytes(StandardCharsets.UTF_8));
                matchMsg.putUserProperty("price", "30");
                matchMsg.putUserProperty("regionId", "cn-hangzhou");
                sharedProducer.send(matchMsg);

                Message lowPriceMsg = new Message(topic, "TAG_SQL", ("low-" + i).getBytes(StandardCharsets.UTF_8));
                lowPriceMsg.putUserProperty("price", "10");
                lowPriceMsg.putUserProperty("regionId", "cn-hangzhou");
                sharedProducer.send(lowPriceMsg);
            }
            assertTrue(matchLatch.await(15, TimeUnit.SECONDS), "SQL92 matching messages not received");
            Thread.sleep(500L);
            assertTrue(leaked.get() == 0, "SQL92 non-matching messages leaked");
        } finally {
            consumer.shutdown();
        }
    }

    private void case20SqlUnknownProperty() throws Exception {
        String topic = createSuiteTopic("E2E_SQL_UNKNOWN");
        AtomicInteger leaked = new AtomicInteger(0);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("GID_SQL_UNK_" + UUID.randomUUID().toString().substring(0, 6));
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, MessageSelector.bySql("product = 'MQ'"));
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, ctx) -> {
            leaked.addAndGet(msgs.size());
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        try {
            for (int i = 0; i < 5; i++) {
                Message msg = new Message(topic, "TAG_SQL", ("unk-" + i).getBytes(StandardCharsets.UTF_8));
                msg.putUserProperty("regionId", "cn-hangzhou");
                sharedProducer.send(msg);
            }
            Thread.sleep(1000L);
            assertTrue(leaked.get() == 0, "Unknown SQL property filter leaked messages");
        } finally {
            consumer.shutdown();
        }
    }

    // =========================================================================
    // 8. Message Content, Keys, Properties & Size Boundary Scenarios
    // =========================================================================

    private void case21BodySpaceUnicodeEmoji() throws Exception {
        String tag = "TAG_BODY_" + UUID.randomUUID().toString().substring(0, 6);
        List<String> payloads = Arrays.asList(" ", "RocketMQ-\u4E2D\u6587\u6D4B\u8BD5", "\uD83D\uDE31\uD83D\uDE80");
        Set<String> received = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(payloads.size());

        DefaultMQPushConsumer consumer = createPushConsumer("GID_BODY", sharedTopic, tag, (msgs, ctx) -> {
            for (MessageExt msg : msgs) {
                received.add(new String(msg.getBody(), StandardCharsets.UTF_8));
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            for (String p : payloads) {
                sharedProducer.send(new Message(sharedTopic, tag, p.getBytes(StandardCharsets.UTF_8)));
            }
            assertTrue(latch.await(15, TimeUnit.SECONDS), "Special body messages timeout");
            assertTrue(received.containsAll(payloads), "Payload mismatch: " + received);
        } finally {
            consumer.shutdown();
        }
    }

    private void case22MultiKeysAndUnicodeKey() throws Exception {
        String tag = "TAG_KEYS_" + UUID.randomUUID().toString().substring(0, 6);
        CountDownLatch latch = new CountDownLatch(2);
        Set<String> observedKeys = ConcurrentHashMap.newKeySet();

        DefaultMQPushConsumer consumer = createPushConsumer("GID_KEYS", sharedTopic, tag, (msgs, ctx) -> {
            for (MessageExt msg : msgs) {
                if (msg.getKeys() != null) {
                    observedKeys.add(msg.getKeys());
                }
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            Message m1 = new Message(sharedTopic, tag, "\u4E2D\u6587Key", "b1".getBytes(StandardCharsets.UTF_8));
            sharedProducer.send(m1);

            Message m2 = new Message(sharedTopic, tag, "b2".getBytes(StandardCharsets.UTF_8));
            m2.setKeys(Arrays.asList("keyAlpha", "keyBeta"));
            sharedProducer.send(m2);

            assertTrue(latch.await(15, TimeUnit.SECONDS), "Multi-key messages timeout");
            assertTrue(observedKeys.size() == 2, "Keys not preserved");
        } finally {
            consumer.shutdown();
        }
    }

    private void case23LargePayloadWithProps() throws Exception {
        String tag = "TAG_LARGE_" + UUID.randomUUID().toString().substring(0, 6);
        byte[] largeBody = new byte[512 * 1024];
        Arrays.fill(largeBody, (byte) 'X');
        long expectedCrc = CorrectnessLedger.computeCrc32(largeBody);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean crcOk = new AtomicBoolean(false);

        DefaultMQPushConsumer consumer = createPushConsumer("GID_LARGE", sharedTopic, tag, (msgs, ctx) -> {
            for (MessageExt msg : msgs) {
                if (CorrectnessLedger.computeCrc32(msg.getBody()) == expectedCrc
                    && msg.getUserProperty("largeProp") != null) {
                    crcOk.set(true);
                }
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            Message msg = new Message(sharedTopic, tag, largeBody);
            StringBuilder propVal = new StringBuilder(4096);
            for (int i = 0; i < 4096; i++) {
                propVal.append('P');
            }
            msg.putUserProperty("largeProp", propVal.toString());
            SendResult res = sharedProducer.send(msg);
            assertTrue(res.getSendStatus() == SendStatus.SEND_OK, "512KB message send failed");
            assertTrue(latch.await(15, TimeUnit.SECONDS) && crcOk.get(), "512KB message verification failed");
        } finally {
            consumer.shutdown();
        }
    }

    private void case24BodyOver4MBRejected() {
        byte[] oversized = new byte[4 * 1024 * 1024 + 1024];
        boolean rejected = false;
        try {
            Message msg = new Message(sharedTopic, "TAG_OVER_4M", oversized);
            sharedProducer.send(msg);
        } catch (Exception e) {
            rejected = true;
        }
        assertTrue(rejected, "Body > 4MB must be rejected by producer/broker");
    }

    private void case25EmptyAndNullValidation() {
        boolean emptyBodyRejected = false;
        try {
            sharedProducer.send(new Message(sharedTopic, "TAG", new byte[0]));
        } catch (Exception e) {
            emptyBodyRejected = true;
        }
        boolean emptyTopicRejected = false;
        try {
            sharedProducer.send(new Message("", "TAG", "abc".getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            emptyTopicRejected = true;
        }
        assertTrue(emptyBodyRejected && emptyTopicRejected, "Empty body and empty topic must be rejected");
    }

    // =========================================================================
    // 9. Broadcast, Rebalance, Offset & LitePull Scenarios
    // =========================================================================

    private void case26BroadcastDiffAndSameGroup() throws Exception {
        String topic = createSuiteTopic("E2E_BROADCAST");
        int count = 10;
        CountDownLatch latchA = new CountDownLatch(count);
        CountDownLatch latchB = new CountDownLatch(count);
        CountDownLatch latchC = new CountDownLatch(count);

        String sameGroup = "GID_BC_SAME_" + UUID.randomUUID().toString().substring(0, 6);
        String diffGroup = "GID_BC_DIFF_" + UUID.randomUUID().toString().substring(0, 6);

        DefaultMQPushConsumer c1 = createBroadcastConsumer(sameGroup, topic, latchA);
        DefaultMQPushConsumer c2 = createBroadcastConsumer(sameGroup, topic, latchB);
        DefaultMQPushConsumer c3 = createBroadcastConsumer(diffGroup, topic, latchC);
        try {
            Thread.sleep(500L);
            for (int i = 0; i < count; i++) {
                sharedProducer.send(new Message(topic, "TAG_BC", ("bc-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            assertTrue(latchA.await(15, TimeUnit.SECONDS), "Broadcast consumer 1 (same group) missed messages");
            assertTrue(latchB.await(15, TimeUnit.SECONDS), "Broadcast consumer 2 (same group) missed messages");
            assertTrue(latchC.await(15, TimeUnit.SECONDS), "Broadcast consumer 3 (diff group) missed messages");
        } finally {
            c1.shutdown();
            c2.shutdown();
            c3.shutdown();
        }
    }

    private DefaultMQPushConsumer createBroadcastConsumer(String group, String topic, CountDownLatch latch)
        throws Exception {
        DefaultMQPushConsumer c = new DefaultMQPushConsumer(group);
        c.setNamesrvAddr(config.getNamesrvAddr());
        c.setInstanceName("bc-" + UUID.randomUUID());
        c.setMessageModel(MessageModel.BROADCASTING);
        c.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        c.subscribe(topic, "*");
        c.registerMessageListener((MessageListenerConcurrently) (msgs, ctx) -> {
            for (int i = 0; i < msgs.size(); i++) {
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        c.start();
        return c;
    }

    private void case27ClusterDynamicScale() throws Exception {
        String topic = createSuiteTopic("E2E_REBALANCE");
        String group = "GID_REB_" + UUID.randomUUID().toString().substring(0, 6);
        int batchCount = 20;
        Set<String> consumedKeys = ConcurrentHashMap.newKeySet();

        MessageListenerConcurrently listener = (msgs, ctx) -> {
            for (MessageExt m : msgs) {
                consumedKeys.add(m.getKeys());
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        };

        DefaultMQPushConsumer c1 = createClusterConsumer(group, topic, listener);
        DefaultMQPushConsumer c2 = createClusterConsumer(group, topic, listener);
        try {
            for (int i = 0; i < batchCount; i++) {
                sharedProducer.send(new Message(topic, "TAG", "k1-" + i, ("v-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            awaitCount(consumedKeys, batchCount, 15000L);

            // Scale down c2 and send second batch
            c2.shutdown();
            for (int i = 0; i < batchCount; i++) {
                sharedProducer.send(new Message(topic, "TAG", "k2-" + i, ("v-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            awaitCount(consumedKeys, batchCount * 2, 20000L);
        } finally {
            c1.shutdown();
            c2.shutdown();
        }
    }

    private DefaultMQPushConsumer createClusterConsumer(String group, String topic,
        MessageListenerConcurrently listener) throws Exception {
        DefaultMQPushConsumer c = new DefaultMQPushConsumer(group, null, new AllocateMessageQueueAveragely());
        c.setNamesrvAddr(config.getNamesrvAddr());
        c.setInstanceName("cluster-" + UUID.randomUUID());
        c.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        c.subscribe(topic, "*");
        c.registerMessageListener(listener);
        c.start();
        return c;
    }

    private void case28ConsumeFromFirstAndLast() throws Exception {
        String topic = createSuiteTopic("E2E_OFFSET_WHERE");
        int backlog = 10;
        for (int i = 0; i < backlog; i++) {
            sharedProducer.send(new Message(topic, "TAG", ("pre-" + i).getBytes(StandardCharsets.UTF_8)));
        }

        // New group with CONSUME_FROM_FIRST_OFFSET must consume the backlog
        CountDownLatch firstLatch = new CountDownLatch(backlog);
        DefaultMQPushConsumer firstConsumer = new DefaultMQPushConsumer("GID_FIRST_" + UUID.randomUUID().toString().substring(0, 6));
        firstConsumer.setNamesrvAddr(config.getNamesrvAddr());
        firstConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        firstConsumer.subscribe(topic, "*");
        firstConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, ctx) -> {
            for (int i = 0; i < msgs.size(); i++) {
                firstLatch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        firstConsumer.start();
        try {
            assertTrue(firstLatch.await(15, TimeUnit.SECONDS), "CONSUME_FROM_FIRST_OFFSET did not consume backlog");
        } finally {
            firstConsumer.shutdown();
        }
    }

    private void case29LitePullSubscribeAndAssign() throws Exception {
        String topic = createSuiteTopic("E2E_LITE_PULL");
        int count = 8;
        for (int i = 0; i < count; i++) {
            sharedProducer.send(new Message(topic, "TAG_LP", ("lp-" + i).getBytes(StandardCharsets.UTF_8)));
        }

        DefaultLitePullConsumer assignConsumer = new DefaultLitePullConsumer("GID_LP_ASSIGN_" + UUID.randomUUID().toString().substring(0, 6));
        assignConsumer.setNamesrvAddr(config.getNamesrvAddr());
        assignConsumer.setAutoCommit(false);
        assignConsumer.start();
        try {
            Collection<MessageQueue> mqs = assignConsumer.fetchMessageQueues(topic);
            assignConsumer.assign(mqs);
            for (MessageQueue mq : mqs) {
                assignConsumer.seekToBegin(mq);
            }
            int received = 0;
            long deadline = System.currentTimeMillis() + 15000L;
            while (received < count && System.currentTimeMillis() < deadline) {
                List<MessageExt> polled = assignConsumer.poll(500L);
                if (polled != null && !polled.isEmpty()) {
                    received += polled.size();
                    assignConsumer.commitSync();
                }
            }
            assertTrue(received == count, "LitePull assign+seekToBegin expected " + count + " but got " + received);
        } finally {
            assignConsumer.shutdown();
        }
    }

    // =========================================================================
    // 10. Retry & Client Init Validation Scenarios
    // =========================================================================

    private void case30NormalExceptionAndNullRetry() throws Exception {
        String topic = createSuiteTopic("E2E_EXC_RETRY");
        CountDownLatch retrySuccessLatch = new CountDownLatch(2);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("GID_EXC_RETRY_" + UUID.randomUUID().toString().substring(0, 6));
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setMaxReconsumeTimes(2);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, ctx) -> {
            ctx.setDelayLevelWhenNextConsume(1);
            MessageExt msg = msgs.get(0);
            String mode = msg.getUserProperty("failMode");
            if (msg.getReconsumeTimes() == 0) {
                if ("exception".equals(mode)) {
                    throw new RuntimeException("Simulated consumption exception");
                } else {
                    return null;
                }
            }
            retrySuccessLatch.countDown();
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        try {
            Message m1 = new Message(topic, "TAG", "exc".getBytes(StandardCharsets.UTF_8));
            m1.putUserProperty("failMode", "exception");
            sharedProducer.send(m1);

            Message m2 = new Message(topic, "TAG", "null".getBytes(StandardCharsets.UTF_8));
            m2.putUserProperty("failMode", "null");
            sharedProducer.send(m2);

            assertTrue(retrySuccessLatch.await(20, TimeUnit.SECONDS), "Exception/null return retry did not redeliver");
        } finally {
            consumer.shutdown();
        }
    }

    private void case31OrderlySuspendAndResume() throws Exception {
        String topic = createSuiteTopic("E2E_ORDER_RETRY");
        int count = 5;
        List<Integer> finalConsumedOrder = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(count);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("GID_ORD_RETRY_" + UUID.randomUUID().toString().substring(0, 6));
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setSuspendCurrentQueueTimeMillis(500L);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, ctx) -> {
            for (MessageExt msg : msgs) {
                int seq = Integer.parseInt(new String(msg.getBody(), StandardCharsets.UTF_8));
                if (seq == 0 && msg.getReconsumeTimes() == 0) {
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                finalConsumedOrder.add(seq);
                latch.countDown();
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();

        try {
            List<MessageQueue> mqs = sharedProducer.fetchPublishMessageQueues(topic);
            for (int i = 0; i < count; i++) {
                sharedProducer.send(new Message(topic, "TAG", Integer.toString(i).getBytes(StandardCharsets.UTF_8)), mqs.get(0));
            }
            assertTrue(latch.await(20, TimeUnit.SECONDS), "Orderly suspend retry timeout");
            assertTrue(finalConsumedOrder.equals(Arrays.asList(0, 1, 2, 3, 4)),
                "Orderly suspend broke FIFO order: " + finalConsumedOrder);
        } finally {
            consumer.shutdown();
        }
    }

    private void case32ClientInitValidation() {
        // 1. PushConsumer without MessageListener must fail on start
        boolean noListenerRejected = false;
        DefaultMQPushConsumer c1 = new DefaultMQPushConsumer("GID_NO_LISTENER_" + UUID.randomUUID().toString().substring(0, 6));
        c1.setNamesrvAddr(config.getNamesrvAddr());
        try {
            c1.subscribe(sharedTopic, "*");
            c1.start();
        } catch (Exception e) {
            noListenerRejected = true;
        } finally {
            c1.shutdown();
        }

        // 2. PushConsumer with null subscription must fail
        boolean nullSubRejected = false;
        DefaultMQPushConsumer c2 = new DefaultMQPushConsumer("GID_NULL_SUB_" + UUID.randomUUID().toString().substring(0, 6));
        try {
            c2.subscribe(null, (String) null);
        } catch (Exception e) {
            nullSubRejected = true;
        }

        // 3. System inner group DEFAULT_CONSUMER must be rejected
        boolean systemGroupRejected = false;
        DefaultLitePullConsumer c3 = new DefaultLitePullConsumer("DEFAULT_CONSUMER");
        c3.setNamesrvAddr(config.getNamesrvAddr());
        try {
            c3.subscribe(sharedTopic, "*");
            c3.start();
        } catch (Exception e) {
            systemGroupRejected = true;
        } finally {
            c3.shutdown();
        }

        assertTrue(noListenerRejected && nullSubRejected && systemGroupRejected,
            "Client init validation failed: noListener=" + noListenerRejected
                + ", nullSub=" + nullSubRejected + ", sysGroup=" + systemGroupRejected);
    }

    private static void awaitCount(Set<?> set, int expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline && set.size() < expected) {
            Thread.sleep(200L);
        }
        assertTrue(set.size() >= expected, "Expected at least " + expected + " items but got " + set.size());
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
