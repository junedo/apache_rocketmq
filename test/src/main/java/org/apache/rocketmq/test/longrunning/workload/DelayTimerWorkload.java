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

public class DelayTimerWorkload implements Workload {

    private static final Logger log = LoggerFactory.getLogger(DelayTimerWorkload.class);
    public static final String WORKLOAD_NAME = "DELAY";

    private final LongRunningConfig config;
    private final CorrectnessLedger ledger;
    private final String topic;
    private final AtomicBoolean producing = new AtomicBoolean(false);
    private final AtomicLong seqGenerator = new AtomicLong(0);

    private DefaultMQProducer producer;
    private DefaultMQPushConsumer consumer;
    private ScheduledExecutorService executor;

    public DelayTimerWorkload(LongRunningConfig config, CorrectnessLedger ledger) {
        this.config = config;
        this.ledger = ledger;
        this.topic = "LR_DELAY_" + config.getRunId().replace('-', '_');
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
        consumer = new DefaultMQPushConsumer("CID_LR_DELAY_" + config.getRunId().replace('-', '_'));
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setInstanceName("consumer-delay-" + UUID.randomUUID());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                ledger.recordConsumed(msg);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        producer = new DefaultMQProducer("PID_LR_DELAY_" + config.getRunId().replace('-', '_'));
        producer.setNamesrvAddr(config.getNamesrvAddr());
        producer.setInstanceName("producer-delay-" + UUID.randomUUID());
        producer.setSendMsgTimeout(5000);
        producer.setRetryTimesWhenSendFailed(3);
        producer.start();

        producing.set(true);
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "DelayWorkloadProducer");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::produceOne, 150, config.getSendIntervalMs() * 2L, TimeUnit.MILLISECONDS);
        log.info("Started workload {} on topic {}", WORKLOAD_NAME, topic);
    }

    private void produceOne() {
        if (!producing.get()) {
            return;
        }
        long seq = seqGenerator.getAndIncrement();
        long deliverTimeMs = System.currentTimeMillis() + config.getDelayMessageMs();
        Message msg = ledger.buildTrackedMessage(topic, "TAG_DELAY", WORKLOAD_NAME, "delay-0", seq, deliverTimeMs);
        msg.setDeliverTimeMs(deliverTimeMs);
        String msgKey = msg.getUserProperty(CorrectnessLedger.PROP_MSG_KEY);
        ledger.registerNonTxSent(msg);
        try {
            SendResult sendResult = producer.send(msg);
            if (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
                ledger.removeUnsent(msgKey);
            }
        } catch (Exception e) {
            ledger.removeUnsent(msgKey);
            log.debug("Delay workload send transient error: {}", e.getMessage());
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
