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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.longrunning.config.LongRunningConfig;
import org.apache.rocketmq.test.longrunning.ledger.CorrectnessLedger;

/**
 * Batch message workload modeled after junedo/rocketmq-e2e BatchProducerTest:
 * alternates between topic-wide batch send and specific MessageQueue batch send.
 */
public class BatchMessageWorkload implements Workload {

    private static final Logger log = LoggerFactory.getLogger(BatchMessageWorkload.class);
    public static final String WORKLOAD_NAME = "BATCH";
    private static final int BATCH_SIZE = 4;

    private final LongRunningConfig config;
    private final CorrectnessLedger ledger;
    private final String topic;
    private final AtomicBoolean producing = new AtomicBoolean(false);
    private final AtomicLong seqGenerator = new AtomicLong(0);
    private final AtomicLong batchRound = new AtomicLong(0);

    private DefaultMQProducer producer;
    private DefaultMQPushConsumer consumer;
    private ScheduledExecutorService executor;

    public BatchMessageWorkload(LongRunningConfig config, CorrectnessLedger ledger) {
        this.config = config;
        this.ledger = ledger;
        this.topic = "LR_BATCH_" + config.getRunId().replace('-', '_');
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
        consumer = new DefaultMQPushConsumer("CID_LR_BATCH_" + config.getRunId().replace('-', '_'));
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setInstanceName("consumer-batch-" + UUID.randomUUID());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                ledger.recordConsumed(msg);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        producer = new DefaultMQProducer("PID_LR_BATCH_" + config.getRunId().replace('-', '_'));
        producer.setNamesrvAddr(config.getNamesrvAddr());
        producer.setInstanceName("producer-batch-" + UUID.randomUUID());
        producer.setSendMsgTimeout(5000);
        producer.setRetryTimesWhenSendFailed(3);
        producer.start();

        producing.set(true);
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "BatchWorkloadProducer");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::produceBatch, 120, config.getSendIntervalMs() * 4L, TimeUnit.MILLISECONDS);
        log.info("Started workload {} on topic {}", WORKLOAD_NAME, topic);
    }

    private void produceBatch() {
        if (!producing.get()) {
            return;
        }
        long round = batchRound.getAndIncrement();
        List<Message> batch = new ArrayList<>(BATCH_SIZE);
        List<String> keys = new ArrayList<>(BATCH_SIZE);
        for (int i = 0; i < BATCH_SIZE; i++) {
            long seq = seqGenerator.getAndIncrement();
            Message msg = ledger.buildTrackedMessage(topic, "TAG_BATCH", WORKLOAD_NAME, "batch-0", seq, 0L);
            String msgKey = msg.getUserProperty(CorrectnessLedger.PROP_MSG_KEY);
            ledger.registerNonTxSent(msg);
            batch.add(msg);
            keys.add(msgKey);
        }

        try {
            SendResult sendResult;
            if (round % 2 == 0) {
                sendResult = producer.send(batch);
            } else {
                List<MessageQueue> mqs = producer.fetchPublishMessageQueues(topic);
                if (mqs != null && !mqs.isEmpty()) {
                    MessageQueue mq = mqs.get((int) (round % mqs.size()));
                    sendResult = producer.send(batch, mq);
                } else {
                    sendResult = producer.send(batch);
                }
            }
            if (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
                for (String k : keys) {
                    ledger.removeUnsent(k);
                }
            }
        } catch (Exception e) {
            for (String k : keys) {
                ledger.removeUnsent(k);
            }
            log.debug("Batch workload send transient error: {}", e.getMessage());
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
