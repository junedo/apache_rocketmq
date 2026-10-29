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
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.longrunning.config.LongRunningConfig;
import org.apache.rocketmq.test.longrunning.ledger.CorrectnessLedger;

public class TransactionWorkload implements Workload {

    private static final Logger log = LoggerFactory.getLogger(TransactionWorkload.class);
    public static final String WORKLOAD_NAME = "TRANSACTION";
    private static final String PROP_TX_ACTION = "LR_TX_ACTION";

    private final LongRunningConfig config;
    private final CorrectnessLedger ledger;
    private final String topic;
    private final AtomicBoolean producing = new AtomicBoolean(false);
    private final AtomicLong seqGenerator = new AtomicLong(0);

    private TransactionMQProducer producer;
    private DefaultMQPushConsumer consumer;
    private ScheduledExecutorService executor;

    public TransactionWorkload(LongRunningConfig config, CorrectnessLedger ledger) {
        this.config = config;
        this.ledger = ledger;
        this.topic = "LR_TX_" + config.getRunId().replace('-', '_');
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
        consumer = new DefaultMQPushConsumer("CID_LR_TX_" + config.getRunId().replace('-', '_'));
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setInstanceName("consumer-tx-" + UUID.randomUUID());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                ledger.recordConsumed(msg);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

        producer = new TransactionMQProducer("PID_LR_TX_" + config.getRunId().replace('-', '_'));
        producer.setNamesrvAddr(config.getNamesrvAddr());
        producer.setInstanceName("producer-tx-" + UUID.randomUUID());
        producer.setSendMsgTimeout(5000);
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                String action = (String) arg;
                String msgKey = msg.getUserProperty(CorrectnessLedger.PROP_MSG_KEY);
                if ("COMMIT".equals(action)) {
                    ledger.markTxCommitted(msgKey);
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if ("ROLLBACK".equals(action)) {
                    ledger.markTxRolledBack(msgKey);
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                } else {
                    return LocalTransactionState.UNKNOW;
                }
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                String action = msg.getUserProperty(PROP_TX_ACTION);
                String msgKey = msg.getUserProperty(CorrectnessLedger.PROP_MSG_KEY);
                if ("CHECK_ROLLBACK".equals(action) || "ROLLBACK".equals(action)) {
                    ledger.markTxRolledBack(msgKey);
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
                ledger.markTxCommitted(msgKey);
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();

        producing.set(true);
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "TxWorkloadProducer");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::produceOne, 200, config.getSendIntervalMs() * 3L, TimeUnit.MILLISECONDS);
        log.info("Started workload {} on topic {}", WORKLOAD_NAME, topic);
    }

    private void produceOne() {
        if (!producing.get()) {
            return;
        }
        long seq = seqGenerator.getAndIncrement();
        // Deterministic mix: 70% immediate COMMIT, 20% immediate ROLLBACK, 10% UNKNOWN -> CHECK_COMMIT / CHECK_ROLLBACK
        String action;
        long mod = seq % 10;
        if (mod < 7) {
            action = "COMMIT";
        } else if (mod < 9) {
            action = "ROLLBACK";
        } else {
            action = (seq % 20 == 9) ? "CHECK_COMMIT" : "CHECK_ROLLBACK";
        }

        Message msg = ledger.buildTrackedMessage(topic, "TAG_TX", WORKLOAD_NAME, "tx-0", seq, 0L);
        msg.putUserProperty(PROP_TX_ACTION, action);
        String msgKey = msg.getUserProperty(CorrectnessLedger.PROP_MSG_KEY);
        ledger.registerTxPrepared(msg);
        try {
            TransactionSendResult result = producer.sendMessageInTransaction(msg, action);
            if (result == null || result.getSendStatus() != SendStatus.SEND_OK) {
                ledger.removeUnsent(msgKey);
            }
        } catch (Exception e) {
            ledger.removeUnsent(msgKey);
            log.debug("Transaction workload send transient error: {}", e.getMessage());
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
