package com.anupambasak.processor;

import com.anupambasak.dtos.BaseRecord;
import com.anupambasak.dtos.DataRecord;
import com.anupambasak.dtos.MetadataRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.*;

@Configuration
@Slf4j
public class RecordProcessor {

    @Autowired
    private Serde<BaseRecord> baseRecordSerde;
    @Autowired
    private Serde<DataRecord> dataRecordSerde;
    @Autowired
    private Serde<MetadataRecord> metadataRecordSerde;
    @Autowired
    private Serde<List<DataRecord>> dataRecordListSerde;


    @Bean
    public KStream<String, MetadataRecord> kStream(StreamsBuilder streamsBuilder) {

        // 1. Consume from input topic
        KStream<String, BaseRecord> input = streamsBuilder.stream("jsonMessageTopic",
                Consumed.with(Serdes.String(), baseRecordSerde));

        // 2. Split into two streams
        Map<String, KStream<String, BaseRecord>> branches = input.split(Named.as("branch-"))
                .branch((k, v) -> v instanceof DataRecord, Branched.as("data"))
                .branch((k, v) -> v instanceof MetadataRecord, Branched.as("metadata"))
                .noDefaultBranch();

        // 3. Aggregate DataRecords into a KTable
        KTable<String, List<DataRecord>> dataTable = branches.get("branch-data")
                .mapValues(v -> (DataRecord) v)
//                .groupBy((k, v) -> v.getProducerId(), Grouped.with(Serdes.String(), dataRecordSerde))
                .groupByKey()
                .aggregate(
                        ArrayList::new,
                        (key, value, aggregate) -> {
                            log.info("key: {}, value: {}", key, value);
                            log.info("aggregate contains: {}", aggregate.contains(value));
                            if(!aggregate.contains(value)) {
                                aggregate.add(value);
                            }
                            return aggregate;
                        },
                        Materialized.<String, List<DataRecord>, KeyValueStore<Bytes, byte[]>>as("data-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(dataRecordListSerde)
                );

        // 4. Join MetadataRecord stream with the KTable
        return branches.get("branch-metadata")
                .mapValues(v -> (MetadataRecord) v)
                .selectKey((k, v) -> v.getProducerId())
                .join(dataTable,
                        (metadata, dataList) -> {
                            if (dataList.size() == metadata.getTotalRecords()) {
                                log.info("Complete batch received for producerId: {}. Found {} records. Can proceed with processing.",
                                        metadata.getProducerId(), dataList.size());
                            } else {
                                log.warn("Record count mismatch for producerId: {}. Expected: {}, Found: {}",
                                        metadata.getProducerId(), metadata.getTotalRecords(), dataList.size());
                            }
                            return metadata;
                        },
                        Joined.with(Serdes.String(), metadataRecordSerde, dataRecordListSerde)
                );
    }
}