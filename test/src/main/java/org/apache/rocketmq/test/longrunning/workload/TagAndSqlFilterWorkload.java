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

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.longrunning.config.LongRunningConfig;
import org.apache.rocketmq.test.longrunning.ledger.CorrectnessLedger;

/**
 * Tag & SQL92 filter workload modeled after junedo/rocketmq-e2e TagFilterTest and SqlFilterTest:
 * - Tests tag hash collision ("BB" vs "Aa", both have hashCode 2112)
 * - Tests tag case sensitivity ("BB" vs "bb")
 * - Tests SQL92 numeric BETWEEN and string equality filtering
 * - Asserts 100% delivery of matching messages and 0% leak of filtered-out messages.
 */
public class TagAndSqlFilterWorkload implements Workload {

    private static final Logger log = LoggerFactory.getLogger(TagAndSqlFilterWorkload.class);
    public static final String WORKLOAD_NAME = "FILTER";

    private static final String TAG_MATCH = "BB";
    private static final String TAG_COLLISION_MISMATCH = "Aa";
    private static final String TAG_CASE_MISMATCH = "bb";

    private final LongRunningConfig config;
    private final CorrectnessLedger ledger;
    private final String topic;
    private final AtomicBoolean producing = new AtomicBoolean(false);
    private final AtomicLong seqGenerator = new AtomicLong(0);

    private DefaultMQProducer producer;
    private DefaultMQPushConsumer consumer;
    private ScheduledExecutorService executor;

    public TagAndSqlFilterWorkload(LongRunningConfig config, CorrectnessLedger ledger) {
        this.config = config;
        this.ledger = ledger;
        this.topic = "LR_FILTER_" + config.getRunId().replace('-', '_');
    }

    @Override
    public String name() {
        return WORKLOAD_NAME;
    }

    @Override
    public String topicName() {
        return topic;
    }

    @Override
    public void start() throws Exception {
        String sqlExpr = "(TAGS IS NOT NULL AND TAGS = 'BB') AND (price BETWEEN 20 AND 100) AND (regionId = 'cn-hangzhou')";
        consumer = new DefaultMQPushConsumer("CID_LR_FILTER_" + config.getRunId().replace('-', '_'));
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setInstanceName("consumer-filter-" + UUID.randomUUID());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, MessageSelector.bySql(sqlExpr));
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                ledger.recordConsumed(msg);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        producer = new DefaultMQProducer("PID_LR_FILTER_" + config.getRunId().replace('-', '_'));
        producer.setNamesrvAddr(config.getNamesrvAddr());
        producer.setInstanceName("producer-filter-" + UUID.randomUUID());
        producer.setSendMsgTimeout(5000);
        producer.setRetryTimesWhenSendFailed(3);
        producer.start();

        producing.set(true);
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "FilterWorkloadProducer");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::produceOne, 140, config.getSendIntervalMs() * 2L, TimeUnit.MILLISECONDS);
        log.info("Started workload {} on topic {}", WORKLOAD_NAME, topic);
    }

    private void produceOne() {
        if (!producing.get()) {
            return;
        }
        long seq = seqGenerator.getAndIncrement();
        long mod = seq % 4;

        String tag;
        String price;
        String regionId;
        boolean shouldMatch;

        if (mod == 0) {
            // Matching message
            tag = TAG_MATCH;
            price = "50";
            regionId = "cn-hangzhou";
            shouldMatch = true;
        } else if (mod == 1) {
            // Same hashCode as "BB" (2112), must be filtered out
            tag = TAG_COLLISION_MISMATCH;
            price = "50";
            regionId = "cn-hangzhou";
            shouldMatch = false;
        } else if (mod == 2) {
            // Case-sensitive mismatch ("bb" vs "BB"), must be filtered out
            tag = TAG_CASE_MISMATCH;
            price = "50";
            regionId = "cn-hangzhou";
            shouldMatch = false;
        } else {
            // SQL property mismatch (price=10 < 20), must be filtered out
            tag = TAG_MATCH;
            price = "10";
            regionId = "cn-shanghai";
            shouldMatch = false;
        }

        Message msg = ledger.buildTrackedMessage(topic, tag, WORKLOAD_NAME, "filter-0", seq, 0L);
        msg.putUserProperty("price", price);
        msg.putUserProperty("regionId", regionId);
        String msgKey = msg.getUserProperty(CorrectnessLedger.PROP_MSG_KEY);

        if (shouldMatch) {
            ledger.registerNonTxSent(msg);
        } else {
            ledger.registerFilteredOutSent(msg);
        }

        try {
            SendResult sendResult = producer.send(msg);
            if (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
                ledger.removeUnsent(msgKey);
            }
        } catch (Exception e) {
            ledger.removeUnsent(msgKey);
            log.debug("Filter workload send transient error: {}", e.getMessage());
        }
    }

    @Override
    public void stopProducing() {
        producing.set(false);
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void shutdown() {
        stopProducing();
        if (producer != null) {
            producer.shutdown();
        }
        if (consumer != null) {
            consumer.shutdown();
        }
    }
}
