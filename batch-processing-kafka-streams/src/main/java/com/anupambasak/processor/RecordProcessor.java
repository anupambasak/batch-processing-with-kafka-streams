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
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

        // 3. Aggregate DataRecords using SessionWindows
        KTable<Windowed<String>, List<DataRecord>> dataTable = branches.get("branch-data")
                .mapValues(v -> (DataRecord) v)
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(90), Duration.ofSeconds(10)))
                .aggregate(
                        ArrayList::new,
                        (key, value, aggregate) -> {
//                            log.info("key: {}, value: {}", key, value);
//                            log.info("aggregate contains: {}", aggregate.contains(value));
                            if(!aggregate.contains(value)) {
                                aggregate.add(value);
                            }
                            return aggregate;
                        },
                        (aggKey, aggOne, aggTwo) -> {
                            aggOne.addAll(aggTwo);
                            return aggOne;
                        },
                        Materialized.<String, List<DataRecord>, SessionStore<Bytes, byte[]>>as("data-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(dataRecordListSerde)
                                .withRetention(Duration.ofMinutes(5))
                );

        // 4. Join MetadataRecord stream with the aggregated data
        KStream<String, MetadataRecord> metadataStream = branches.get("branch-metadata")
                .mapValues(v -> (MetadataRecord) v);

        dataTable.toStream()
                .selectKey((key, value) -> key.key())
//                .peek((key, value) -> log.info("Window closed for producerId: {}", key))
                .join(metadataStream,
                        (data, metadata) -> {
                            if (data.size() == metadata.getTotalRecords()) {
                                log.info("Complete batch received for producerId: {}. Found {} records. Can proceed with processing.",
                                        metadata.getProducerId(), data.size());
                            } else {
                                log.warn("Record count mismatch for producerId: {}. Expected: {}, Found: {}",
                                        metadata.getProducerId(), metadata.getTotalRecords(), data.size());
                            }
                            return metadata;
                        },
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)),
                        StreamJoined.with(
                                Serdes.String(),
                                dataRecordListSerde,
                                metadataRecordSerde
                        )
                );

        return metadataStream;
    }
}