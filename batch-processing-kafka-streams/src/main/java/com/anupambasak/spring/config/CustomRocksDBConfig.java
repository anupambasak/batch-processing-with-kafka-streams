package com.anupambasak.spring.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.*;

import java.util.Map;

@Slf4j
public class CustomRocksDBConfig implements RocksDBConfigSetter {

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {

        log.info("RocksDBConfigSetter setting for store: {}",storeName);

//        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
//                .setBlockSize(16 * 1024)                 // 16 KB (range-scan friendly)
//                .setCacheIndexAndFilterBlocks(true)
//                .setPinL0FilterAndIndexBlocksInCache(true)
//                .setFilterPolicy(new BloomFilter(10, false));

        // Use a Bloom Filter to speed up 'fetch' calls (O(1) check before O(N) scan)
//        tableConfig.setFilterPolicy(new org.rocksdb.BloomFilter(10, false));

        // Increase block cache so your "latest" data stays in memory
//        tableConfig.setBlockCacheSize(64 * 1024 * 1024L); // 64 MB


//        long blockCacheSize = 512L * 1024 * 1024; // 512 MB
//        LRUCache cache = new LRUCache(blockCacheSize);
//        tableConfig.setBlockCache(cache);
//
//        options.setTableFormatConfig(tableConfig);
//
//        options.useFixedLengthPrefixExtractor(16);

//        options.setCompactionStyle(CompactionStyle.LEVEL);
//        options.setTargetFileSizeBase(256 * 1024 * 1024); // 256 MB
//        options.setMaxBytesForLevelBase(1024 * 1024 * 1024L); // 1 GB
//        options.setLevel0FileNumCompactionTrigger(4);


        options.setTtl(120);

    }

    @Override
    public void close(String storeName, Options options) {

    }
}
