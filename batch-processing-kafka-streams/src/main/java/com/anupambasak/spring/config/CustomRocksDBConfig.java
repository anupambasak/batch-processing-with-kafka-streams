package com.anupambasak.spring.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@Slf4j
public class CustomRocksDBConfig implements RocksDBConfigSetter {

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
//        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
//
//        // Use a Bloom Filter to speed up 'fetch' calls (O(1) check before O(N) scan)
//        tableConfig.setFilterPolicy(new org.rocksdb.BloomFilter(10, false));
//
//        // Increase block cache so your "latest" data stays in memory
//        tableConfig.setBlockCacheSize(64 * 1024 * 1024L); // 64 MB
//
//        options.setTableFormatConfig(tableConfig);
        options.setTtl(120);
        log.info("RocksDBConfigSetter setting TTL of {} for store: {}",120,storeName);
    }

    @Override
    public void close(String storeName, Options options) {

    }
}
