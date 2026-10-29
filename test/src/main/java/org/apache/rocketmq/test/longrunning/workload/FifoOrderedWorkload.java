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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
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

public class FifoOrderedWorkload implements Workload {

    private static final Logger log = LoggerFactory.getLogger(FifoOrderedWorkload.class);
    public static final String WORKLOAD_NAME = "FIFO";

    private final LongRunningConfig config;
    private final CorrectnessLedger ledger;
    private final String topic;
    private final AtomicBoolean producing = new AtomicBoolean(false);
    private final AtomicInteger keyRoundRobin = new AtomicInteger(0);
    private final ConcurrentMap<String, AtomicLong> keySeqGenerators = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MessageQueue> pinnedQueues = new ConcurrentHashMap<>();

    private DefaultMQProducer producer;
    private DefaultMQPushConsumer consumer;
    private ScheduledExecutorService executor;

    public FifoOrderedWorkload(LongRunningConfig config, CorrectnessLedger ledger) {
        this.config = config;
        this.ledger = ledger;
        this.topic = "LR_FIFO_" + config.getRunId().replace('-', '_');
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
        consumer = new DefaultMQPushConsumer("CID_LR_FIFO_" + config.getRunId().replace('-', '_'));
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setInstanceName("consumer-fifo-" + UUID.randomUUID());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
            context.setAutoCommit(true);
            for (MessageExt msg : msgs) {
                ledger.recordConsumed(msg);
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();

        producer = new DefaultMQProducer("PID_LR_FIFO_" + config.getRunId().replace('-', '_'));
        producer.setNamesrvAddr(config.getNamesrvAddr());
        producer.setInstanceName("producer-fifo-" + UUID.randomUUID());
        producer.setSendMsgTimeout(5000);
        producer.start();

        producing.set(true);
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "FifoWorkloadProducer");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::produceOne, 100, config.getSendIntervalMs() * 2L, TimeUnit.MILLISECONDS);
        log.info("Started workload {} on topic {}", WORKLOAD_NAME, topic);
    }

    private void produceOne() {
        if (!producing.get()) {
            return;
        }
        int keyIdx = (keyRoundRobin.getAndIncrement() & 0x7fffffff) % config.getFifoKeyCount();
        String shardKey = "fifo-key-" + keyIdx;
        AtomicLong seqHolder = keySeqGenerators.computeIfAbsent(shardKey, k -> new AtomicLong(0));
        long nextSeq = seqHolder.get();

        Message msg = ledger.buildTrackedMessage(topic, "TAG_FIFO", WORKLOAD_NAME, shardKey, nextSeq, 0L);
        String msgKey = msg.getUserProperty(CorrectnessLedger.PROP_MSG_KEY);
        ledger.registerNonTxSent(msg);

        MessageQueueSelector selector = (List<MessageQueue> mqs, Message m, Object arg) -> {
            String key = (String) arg;
            MessageQueue pinned = pinnedQueues.get(key);
            if (pinned != null) {
                for (MessageQueue mq : mqs) {
                    if (mq.equals(pinned)) {
                        return mq;
                    }
                }
            }
            int idx = (key.hashCode() & 0x7fffffff) % mqs.size();
            MessageQueue chosen = mqs.get(idx);
            pinnedQueues.putIfAbsent(key, chosen);
            return chosen;
        };

        try {
            SendResult sendResult = producer.send(msg, selector, shardKey);
            if (sendResult != null && sendResult.getSendStatus() == SendStatus.SEND_OK) {
                seqHolder.incrementAndGet();
            } else {
                ledger.removeUnsent(msgKey);
            }
        } catch (Exception e) {
            ledger.removeUnsent(msgKey);
            log.debug("FIFO workload send transient error on shard {}: {}", shardKey, e.getMessage());
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
