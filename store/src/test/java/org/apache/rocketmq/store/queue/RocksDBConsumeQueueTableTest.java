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
package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;

import static org.apache.rocketmq.store.queue.RocksDBConsumeQueueTable.CQ_UNIT_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RocksDBConsumeQueueTableTest {

    @Test
    public void testBinarySearchInCQByTime() throws RocksDBException {
        ConsumeQueueRocksDBStorage rocksDBStorage = mock(ConsumeQueueRocksDBStorage.class);
        DefaultMessageStore store = mock(DefaultMessageStore.class);
        RocksDBConsumeQueueTable table = new RocksDBConsumeQueueTable(rocksDBStorage, store);
        doAnswer((Answer<byte[]>) mock -> {
            /*
             * queueOffset timestamp
             * 100         1000
             * 200         2000
             * 201         2010
             * 1000        10000
             */
            byte[] keyBytes = mock.getArgument(0);
            ByteBuffer keyBuffer = ByteBuffer.wrap(keyBytes);
            int len = keyBuffer.getInt(0);
            long offset = keyBuffer.getLong(4 + 1 + len + 1 + 4 + 1);
            long phyOffset = offset;
            long timestamp = offset * 10;
            final ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_UNIT_SIZE);
            byteBuffer.putLong(phyOffset);
            byteBuffer.putInt(1);
            byteBuffer.putLong(0);
            byteBuffer.putLong(timestamp);
            return byteBuffer.array();
        }).when(rocksDBStorage).getCQ(any());
        assertEquals(1001, table.binarySearchInCQByTime("topic", 0, 1000, 100, 20000, 0, BoundaryType.LOWER));
        assertEquals(1000, table.binarySearchInCQByTime("topic", 0, 1000, 100, 20000, 0, BoundaryType.UPPER));
        assertEquals(100, table.binarySearchInCQByTime("topic", 0, 1000, 100, 1, 0, BoundaryType.LOWER));
        assertEquals(0, table.binarySearchInCQByTime("topic", 0, 1000, 100, 1, 0, BoundaryType.UPPER));
        assertEquals(201, table.binarySearchInCQByTime("topic", 0, 1000, 100, 2001, 0, BoundaryType.LOWER));
        assertEquals(200, table.binarySearchInCQByTime("topic", 0, 1000, 100, 2001, 0, BoundaryType.UPPER));
        assertEquals(200, table.binarySearchInCQByTime("topic", 0, 1000, 100, 2000, 0, BoundaryType.LOWER));
        assertEquals(200, table.binarySearchInCQByTime("topic", 0, 1000, 100, 2000, 0, BoundaryType.UPPER));
    }

    @Test
    public void testGetCQInKVZeroCopyWithRealRocksDB() throws Exception {
        String dbPath = Files.createTempDirectory("rocksdb-cq-table-test-" + UUID.randomUUID()).toString();
        DefaultMessageStore store = mock(DefaultMessageStore.class);
        when(store.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(store.getMinPhyOffset()).thenReturn(0L);
        ConsumeQueueRocksDBStorage storage = new ConsumeQueueRocksDBStorage(store, dbPath);
        try {
            assertTrue(storage.start());
            RocksDBConsumeQueueTable table = new RocksDBConsumeQueueTable(storage, store);
            table.load();

            Pair<ByteBuffer, ByteBuffer> cqBBPair = RocksDBConsumeQueueTable.getCQByteBufferPair();
            byte[] topicBytes = "TopicZeroCopy".getBytes(StandardCharsets.UTF_8);
            try (WriteBatch writeBatch = new WriteBatch()) {
                for (int i = 0; i < 10; i++) {
                    DispatchEntry entry = new DispatchEntry();
                    entry.topic = topicBytes;
                    entry.queueId = 0;
                    entry.queueOffset = i;
                    entry.commitLogOffset = i * 100L;
                    entry.messageSize = 100;
                    entry.tagCode = i + 1L;
                    entry.storeTimestamp = 1000L + i * 10L;
                    table.buildAndPutCQByteBuffer(cqBBPair, entry, writeBatch);
                }
                storage.batchPut(writeBatch);
            }

            ByteBuffer cqBuf = table.getCQInKV("TopicZeroCopy", 0, 5);
            assertNotNull(cqBuf);
            assertTrue(cqBuf.isDirect());
            assertEquals(500L, cqBuf.getLong());
            assertEquals(100, cqBuf.getInt());
            assertEquals(6L, cqBuf.getLong());
            assertEquals(1050L, cqBuf.getLong());

            assertNull(table.getCQInKV("TopicZeroCopy", 0, 99));
            assertEquals(5, table.binarySearchInCQByTime("TopicZeroCopy", 0, 9, 0, 1050L, 0, BoundaryType.LOWER));
        } finally {
            storage.shutdown();
            storage.destroy();
        }
    }
}