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

    private static final String DATA_STORE = "data-store";

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

        // 3. Aggregate DataRecords
        KTable<Windowed<String>, List<DataRecord>> dataTable = branches.get("branch-data")
                .mapValues(v -> (DataRecord) v)
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30)))
                .aggregate(
                        ArrayList::new,
                        (key, value, aggregate) -> {
                            List<DataRecord> updated = new ArrayList<>(aggregate);
                            updated.add(value);
                            return updated;
                        },
                        (key, left, right) -> {
                            List<DataRecord> merged = new ArrayList<>(left);
                            merged.addAll(right);
                            return merged;
                        },
                        Materialized.<String, List<DataRecord>, SessionStore<Bytes, byte[]>>as(DATA_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(dataRecordListSerde)
                                .withRetention(Duration.ofMinutes(3))
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().withMaxRecords(15000)));

        // 4. Join MetadataRecord stream with the aggregated data
        return branches.get("branch-metadata")
                .mapValues(v -> (MetadataRecord) v)
                .join(
                        dataTable.toStream((k, v) -> k.key()),
                        (metadata, data) -> {
                            log.info("inside ValueJoiner");
                            if (data != null && data.size() == metadata.getTotalRecords()) {
                                log.info("Complete batch received for producerId: {}. Found {} records. Can proceed with processing.",
                                        metadata.getProducerId(), data.size());
                            } else {
                                log.warn("Record count mismatch for producerId: {}. Expected: {}, Found: {}",
                                        metadata.getProducerId(), metadata.getTotalRecords(), data != null ? data.size() : "null");
                            }
                            return metadata;
                        },
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(100)),
                        StreamJoined.with(Serdes.String(), metadataRecordSerde, dataRecordListSerde)
                );
    }
}