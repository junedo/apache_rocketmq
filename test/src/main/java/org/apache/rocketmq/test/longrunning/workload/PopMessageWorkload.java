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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.test.client.rmq.RMQPopClient;
import org.apache.rocketmq.test.longrunning.config.LongRunningConfig;
import org.apache.rocketmq.test.longrunning.ledger.CorrectnessLedger;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class PopMessageWorkload implements Workload {

    private static final Logger log = LoggerFactory.getLogger(PopMessageWorkload.class);
    public static final String WORKLOAD_NAME = "POP";

    private final LongRunningConfig config;
    private final CorrectnessLedger ledger;
    private final String topic;
    private final String consumerGroup;
    private final AtomicBoolean producing = new AtomicBoolean(false);
    private final AtomicBoolean consuming = new AtomicBoolean(false);
    private final AtomicLong seqGenerator = new AtomicLong(0);

    private DefaultMQProducer producer;
    private DefaultMQAdminExt adminExt;
    private RMQPopClient popClient;
    private ScheduledExecutorService producerExecutor;
    private ScheduledExecutorService consumerExecutor;

    public PopMessageWorkload(LongRunningConfig config, CorrectnessLedger ledger) {
        this.config = config;
        this.ledger = ledger;
        this.topic = "LR_POP_" + config.getRunId().replace('-', '_');
        this.consumerGroup = "CID_LR_POP_" + config.getRunId().replace('-', '_');
    }

    @Override
    public String name() {
        return WORKLOAD_NAME;
    }

    @Override
    public String topicName() {
        return topic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    @Override
    public void start() throws Exception {
        adminExt = new DefaultMQAdminExt("admin-pop-" + UUID.randomUUID());
        adminExt.setNamesrvAddr(config.getNamesrvAddr());
        adminExt.start();

        popClient = new RMQPopClient();
        popClient.create();
        popClient.start();

        producer = new DefaultMQProducer("PID_LR_POP_" + config.getRunId().replace('-', '_'));
        producer.setNamesrvAddr(config.getNamesrvAddr());
        producer.setInstanceName("producer-pop-" + UUID.randomUUID());
        producer.setSendMsgTimeout(5000);
        producer.setRetryTimesWhenSendFailed(3);
        producer.start();

        producing.set(true);
        consuming.set(true);

        producerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PopWorkloadProducer");
            t.setDaemon(true);
            return t;
        });
        producerExecutor.scheduleWithFixedDelay(this::produceOne, 100, config.getSendIntervalMs() * 2L, TimeUnit.MILLISECONDS);

        consumerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PopWorkloadConsumer");
            t.setDaemon(true);
            return t;
        });
        consumerExecutor.scheduleWithFixedDelay(this::pollPopMessages, 150, 100, TimeUnit.MILLISECONDS);
        log.info("Started workload {} on topic {}", WORKLOAD_NAME, topic);
    }

    private void produceOne() {
        if (!producing.get()) {
            return;
        }
        long seq = seqGenerator.getAndIncrement();
        Message msg = ledger.buildTrackedMessage(topic, "TAG_POP", WORKLOAD_NAME, "pop-0", seq, 0L);
        String msgKey = msg.getUserProperty(CorrectnessLedger.PROP_MSG_KEY);
        ledger.registerNonTxSent(msg);
        try {
            SendResult sendResult = producer.send(msg);
            if (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
                ledger.removeUnsent(msgKey);
            }
        } catch (Exception e) {
            ledger.removeUnsent(msgKey);
            log.debug("Pop workload send transient error: {}", e.getMessage());
        }
    }

    private void pollPopMessages() {
        if (!consuming.get()) {
            return;
        }
        try {
            TopicRouteData routeData = adminExt.examineTopicRouteInfo(topic);
            if (routeData == null || routeData.getBrokerDatas() == null) {
                return;
            }
            for (BrokerData brokerData : routeData.getBrokerDatas()) {
                String brokerAddr = brokerData.selectBrokerAddr();
                if (brokerAddr == null) {
                    continue;
                }
                MessageQueue mq = new MessageQueue(topic, brokerData.getBrokerName(), -1);
                PopResult popResult = popClient.popMessageAsync(
                    brokerAddr, mq, 10000L, 16, consumerGroup, 1500L, false,
                    ConsumeInitMode.MIN, false, ExpressionType.TAG, "*").get(3, TimeUnit.SECONDS);

                if (popResult != null && popResult.getPopStatus() == PopStatus.FOUND
                    && popResult.getMsgFoundList() != null) {
                    List<MessageExt> msgList = popResult.getMsgFoundList();
                    for (MessageExt msgExt : msgList) {
                        ledger.recordConsumed(msgExt);
                        String ck = msgExt.getProperty(MessageConst.PROPERTY_POP_CK);
                        if (ck != null) {
                            popClient.ackMessageAsync(brokerAddr, topic, consumerGroup, ck)
                                .get(3, TimeUnit.SECONDS);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Pop poll transient error (expected during chaos): {}", e.getMessage());
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
        consuming.set(false);
        if (consumerExecutor != null) {
            consumerExecutor.shutdown();
            try {
                consumerExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        if (producer != null) {
            producer.shutdown();
        }
        if (popClient != null) {
            popClient.shutdown();
        }
        if (adminExt != null) {
            adminExt.shutdown();
        }
    }
}
