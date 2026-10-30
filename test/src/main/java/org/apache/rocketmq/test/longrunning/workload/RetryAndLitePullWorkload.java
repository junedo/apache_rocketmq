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

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
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
 * Retry & LitePull workload modeled after junedo/rocketmq-e2e PushConsumerRetryTest and PullAckTest:
 * - Exercises PushConsumer RECONSUME_LATER retry path (first delivery returns RECONSUME_LATER with fast retry,
 *   second delivery succeeds and records consumption in CorrectnessLedger).
 * - Exercises DefaultLitePullConsumer subscribe + poll + commitSync in parallel on a dedicated tag.
 */
public class RetryAndLitePullWorkload implements Workload {

    private static final Logger log = LoggerFactory.getLogger(RetryAndLitePullWorkload.class);
    public static final String WORKLOAD_NAME = "RETRY_PULL";

    private static final String TAG_RETRY = "TAG_RETRY";
    private static final String TAG_LITE_PULL = "TAG_LITE_PULL";

    private final LongRunningConfig config;
    private final CorrectnessLedger ledger;
    private final String topic;
    private final AtomicBoolean producing = new AtomicBoolean(false);
    private final AtomicBoolean pulling = new AtomicBoolean(false);
    private final AtomicLong seqGenerator = new AtomicLong(0);
    private final Set<String> firstAttemptFailedKeys = ConcurrentHashMap.newKeySet();

    private DefaultMQProducer producer;
    private DefaultMQPushConsumer retryPushConsumer;
    private DefaultLitePullConsumer litePullConsumer;
    private ScheduledExecutorService producerExecutor;
    private ScheduledExecutorService litePullExecutor;

    public RetryAndLitePullWorkload(LongRunningConfig config, CorrectnessLedger ledger) {
        this.config = config;
        this.ledger = ledger;
        this.topic = "LR_RETRY_PULL_" + config.getRunId().replace('-', '_');
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
        retryPushConsumer = new DefaultMQPushConsumer("CID_LR_RETRY_" + config.getRunId().replace('-', '_'));
        retryPushConsumer.setNamesrvAddr(config.getNamesrvAddr());
        retryPushConsumer.setInstanceName("consumer-retry-" + UUID.randomUUID());
        retryPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        retryPushConsumer.setMaxReconsumeTimes(3);
        retryPushConsumer.subscribe(topic, TAG_RETRY);
        retryPushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                String msgKey = msg.getUserProperty(CorrectnessLedger.PROP_MSG_KEY);
                long seq = Long.parseLong(msg.getUserProperty(CorrectnessLedger.PROP_SEQ_ID));
                // Force every 3rd message to go through RECONSUME_LATER on first attempt
                if (seq % 3 == 0 && msg.getReconsumeTimes() == 0 && firstAttemptFailedKeys.add(msgKey)) {
                    // delayLevel = 1 (1 second retry)
                    context.setDelayLevelWhenNextConsume(1);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                ledger.recordConsumed(msg);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        retryPushConsumer.start();

        litePullConsumer = new DefaultLitePullConsumer("CID_LR_LITEPULL_" + config.getRunId().replace('-', '_'));
        litePullConsumer.setNamesrvAddr(config.getNamesrvAddr());
        litePullConsumer.setInstanceName("consumer-litepull-" + UUID.randomUUID());
        litePullConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        litePullConsumer.setAutoCommit(false);
        litePullConsumer.setPollTimeoutMillis(500L);
        litePullConsumer.setPullBatchSize(16);
        litePullConsumer.subscribe(topic, TAG_LITE_PULL);
        litePullConsumer.start();

        producer = new DefaultMQProducer("PID_LR_RETRYPULL_" + config.getRunId().replace('-', '_'));
        producer.setNamesrvAddr(config.getNamesrvAddr());
        producer.setInstanceName("producer-retrypull-" + UUID.randomUUID());
        producer.setSendMsgTimeout(5000);
        producer.setRetryTimesWhenSendFailed(3);
        producer.start();

        producing.set(true);
        pulling.set(true);

        producerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "RetryPullWorkloadProducer");
            t.setDaemon(true);
            return t;
        });
        producerExecutor.scheduleWithFixedDelay(this::produceOne, 150, config.getSendIntervalMs() * 2L, TimeUnit.MILLISECONDS);

        litePullExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "LitePullWorkloadConsumer");
            t.setDaemon(true);
            return t;
        });
        litePullExecutor.scheduleWithFixedDelay(this::pollLitePull, 150, 100, TimeUnit.MILLISECONDS);
        log.info("Started workload {} on topic {}", WORKLOAD_NAME, topic);
    }

    private void produceOne() {
        if (!producing.get()) {
            return;
        }
        long seq = seqGenerator.getAndIncrement();
        String tag = (seq % 2 == 0) ? TAG_RETRY : TAG_LITE_PULL;
        Message msg = ledger.buildTrackedMessage(topic, tag, WORKLOAD_NAME, "retrypull-0", seq, 0L);
        String msgKey = msg.getUserProperty(CorrectnessLedger.PROP_MSG_KEY);
        ledger.registerNonTxSent(msg);
        try {
            SendResult sendResult = producer.send(msg);
            if (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
                ledger.removeUnsent(msgKey);
            }
        } catch (Exception e) {
            ledger.removeUnsent(msgKey);
            log.debug("RetryPull workload send transient error: {}", e.getMessage());
        }
    }

    private void pollLitePull() {
        if (!pulling.get()) {
            return;
        }
        try {
            List<MessageExt> msgs = litePullConsumer.poll(300L);
            if (msgs != null && !msgs.isEmpty()) {
                for (MessageExt msg : msgs) {
                    ledger.recordConsumed(msg);
                }
                litePullConsumer.commitSync();
            }
        } catch (Exception e) {
            log.debug("LitePull poll transient error: {}", e.getMessage());
        }
    }

    @Override
    public void stopProducing() {
        producing.set(false);
        if (producerExecutor != null) {
            producerExecutor.shutdown();
            try {
                producerExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void shutdown() {
        stopProducing();
        pulling.set(false);
        if (litePullExecutor != null) {
            litePullExecutor.shutdown();
            try {
                litePullExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        if (producer != null) {
            producer.shutdown();
        }
        if (retryPushConsumer != null) {
            retryPushConsumer.shutdown();
        }
        if (litePullConsumer != null) {
            litePullConsumer.shutdown();
        }
    }
}
