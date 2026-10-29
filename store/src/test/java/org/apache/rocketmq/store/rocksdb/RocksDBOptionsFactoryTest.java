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

package org.apache.rocketmq.store.rocksdb;

import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompressionType;
import org.rocksdb.HyperClockCache;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.util.SizeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RocksDBOptionsFactoryTest {

    @BeforeClass
    public static void loadRocksDB() {
        RocksDB.loadLibrary();
    }

    @Test
    public void testRocksDBVersion11() {
        RocksDB.Version version = RocksDB.rocksdbVersion();
        Assert.assertNotNull(version);
        Assert.assertEquals(11, version.getMajor());
        Assert.assertEquals(8, version.getMinor());
    }

    @Test
    public void testCreateBlockCacheHyperClockAndLRU() {
        try (Cache hyperClock = RocksDBOptionsFactory.createBlockCache(64 * SizeUnit.MB, true);
             Cache lru = RocksDBOptionsFactory.createBlockCache(64 * SizeUnit.MB, false)) {
            Assert.assertTrue(hyperClock instanceof HyperClockCache);
            Assert.assertTrue(lru instanceof LRUCache);
        }
    }

    @Test
    public void testPartitionedIndexFiltersAndFormatVersion() {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setEnableRocksdbHyperClockCache(true);
        config.setEnableRocksdbPartitionedIndexFilters(true);
        config.setRocksdbFormatVersion(6);
        MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(config);

        try (ColumnFamilyOptions options = RocksDBOptionsFactory.createCQCFOptions(messageStore)) {
            Assert.assertTrue(options.tableFormatConfig() instanceof BlockBasedTableConfig);
            BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
            Assert.assertEquals(6, tableConfig.formatVersion());
            Assert.assertEquals(IndexType.kTwoLevelIndexSearch, tableConfig.indexType());
            Assert.assertTrue(tableConfig.partitionFilters());
            Assert.assertTrue(tableConfig.optimizeFiltersForMemory());
        }
    }

    @Test
    public void testBottomMostCompressionType() {
        MessageStoreConfig config = new MessageStoreConfig();
        Assert.assertEquals(CompressionType.ZSTD_COMPRESSION,
            CompressionType.getCompressionType(config.getBottomMostCompressionTypeForConsumeQueueStore()));
        Assert.assertEquals(CompressionType.LZ4_COMPRESSION, CompressionType.getCompressionType("lz4"));
    }

    @Test
    public void testConsumeQueueUniversalCompactionMaxSizeAmplificationPercent() {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setRocksdbMaxSizeAmplificationPercent(50);
        MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(config);

        try (ColumnFamilyOptions options = RocksDBOptionsFactory.createCQCFOptions(messageStore);
            CompactionOptionsUniversal compactionOptions = options.compactionOptionsUniversal()) {
            Assert.assertEquals(50, compactionOptions.maxSizeAmplificationPercent());
        }
    }

}
