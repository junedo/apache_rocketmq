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
package org.apache.rocketmq.test.longrunning;

import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.test.longrunning.config.LongRunningConfig;
import org.apache.rocketmq.test.longrunning.config.StorageProfile;
import org.apache.rocketmq.test.longrunning.ledger.CorrectnessLedger;
import org.apache.rocketmq.test.longrunning.ledger.VerificationReport;
import org.junit.Assert;
import org.junit.Test;

public class CorrectnessLedgerTest {

    @Test
    public void testAllInvariantsPassWhenHealthy() {
        LongRunningConfig config = new LongRunningConfig();
        config.setRunId("test-run-healthy");
        config.setProfile(StorageProfile.ROCKSDB);
        CorrectnessLedger ledger = new CorrectnessLedger(config);

        // 1. Normal messages
        for (int i = 0; i < 10; i++) {
            Message msg = ledger.buildTrackedMessage("T_NORMAL", "TAG", "NORMAL", "s0", i, 0L);
            ledger.registerNonTxSent(msg);
            ledger.recordConsumed(toMessageExt(msg));
        }

        // 2. FIFO ordered messages
        for (int i = 0; i < 5; i++) {
            Message msg = ledger.buildTrackedMessage("T_FIFO", "TAG", "FIFO", "fifo-key-1", i, 0L);
            ledger.registerNonTxSent(msg);
            ledger.recordConsumed(toMessageExt(msg));
        }

        // 3. Transaction messages (1 committed, 1 rolled back)
        Message txCommit = ledger.buildTrackedMessage("T_TX", "TAG", "TRANSACTION", "tx-0", 0, 0L);
        ledger.registerTxPrepared(txCommit);
        ledger.markTxCommitted(txCommit.getUserProperty(CorrectnessLedger.PROP_MSG_KEY));
        ledger.recordConsumed(toMessageExt(txCommit));

        Message txRollback = ledger.buildTrackedMessage("T_TX", "TAG", "TRANSACTION", "tx-0", 1, 0L);
        ledger.registerTxPrepared(txRollback);
        ledger.markTxRolledBack(txRollback.getUserProperty(CorrectnessLedger.PROP_MSG_KEY));

        VerificationReport report = ledger.buildReport(1000L);
        Assert.assertTrue(report.isPassed());
        Assert.assertEquals(16L, report.getTotalExpectedDelivered());
        Assert.assertEquals(16L, report.getTotalUniqueConsumed());
        Assert.assertEquals(1L, report.getTotalRolledBack());
        Assert.assertEquals(0L, report.getUnconsumedCount());
        Assert.assertEquals(0L, report.getCrcCorruptionCount());
        Assert.assertEquals(0L, report.getFifoOrderViolationCount());
        Assert.assertEquals(0L, report.getRollbackLeakCount());
    }

    @Test
    public void testDetectsCrcCorruptionAndRollbackLeakAndFifoGap() {
        LongRunningConfig config = new LongRunningConfig();
        config.setRunId("test-run-violations");
        CorrectnessLedger ledger = new CorrectnessLedger(config);

        // 1. Corrupted payload
        Message corrupted = ledger.buildTrackedMessage("T_NORMAL", "TAG", "NORMAL", "s0", 0, 0L);
        ledger.registerNonTxSent(corrupted);
        MessageExt corruptedExt = toMessageExt(corrupted);
        corruptedExt.setBody("tampered-body".getBytes(StandardCharsets.UTF_8));
        ledger.recordConsumed(corruptedExt);

        // 2. Leaked rollback message
        Message leakedTx = ledger.buildTrackedMessage("T_TX", "TAG", "TRANSACTION", "tx-0", 0, 0L);
        ledger.registerTxPrepared(leakedTx);
        ledger.markTxRolledBack(leakedTx.getUserProperty(CorrectnessLedger.PROP_MSG_KEY));
        ledger.recordConsumed(toMessageExt(leakedTx));

        // 3. FIFO gap (send 0 and 1, only consume 1)
        Message fifo0 = ledger.buildTrackedMessage("T_FIFO", "TAG", "FIFO", "fifo-key-0", 0, 0L);
        Message fifo1 = ledger.buildTrackedMessage("T_FIFO", "TAG", "FIFO", "fifo-key-0", 1, 0L);
        ledger.registerNonTxSent(fifo0);
        ledger.registerNonTxSent(fifo1);
        ledger.recordConsumed(toMessageExt(fifo1));

        VerificationReport report = ledger.buildReport(500L);
        Assert.assertFalse(report.isPassed());
        Assert.assertEquals(1L, report.getCrcCorruptionCount());
        Assert.assertEquals(1L, report.getRollbackLeakCount());
        Assert.assertEquals(1L, report.getFifoOrderViolationCount());
        Assert.assertEquals(1L, report.getFifoGapViolationCount());
        Assert.assertEquals(1L, report.getUnconsumedCount());
    }

    private static MessageExt toMessageExt(Message msg) {
        MessageExt ext = new MessageExt();
        ext.setTopic(msg.getTopic());
        ext.setBody(msg.getBody());
        MessageAccessor.setProperties(ext, msg.getProperties());
        return ext;
    }
}
