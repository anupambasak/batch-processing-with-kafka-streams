package com.anupambasak.processor;

import com.anupambasak.dtos.BaseRecord;
import com.anupambasak.dtos.BatchRecord;
import com.anupambasak.dtos.DataRecord;
import com.anupambasak.dtos.MetadataRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.ArrayList;

@Configuration
@Slf4j
public class RecordProcessorV1 {

    private static final String INPUT_TOPIC = "jsonMessageTopicV1";
    private static final String DATA_STORE = "data-storeV1";

    @Autowired
    private Serde<BaseRecord> baseRecordSerde;

    @Autowired
    private Serde<BatchRecord> batchRecordSerde;

    @Bean
    public Topology topology(StreamsBuilder builder) {

        // 1. Consume input topic
        KStream<String, BaseRecord> input = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), baseRecordSerde));

        // 2. Split by type
        KStream<String, DataRecord> dataStream = input.filter((k, v) -> v instanceof DataRecord)
                .mapValues(v -> (DataRecord) v);

        KStream<String, MetadataRecord> metadataStream = input.filter((k, v) -> v instanceof MetadataRecord)
                .mapValues(v -> (MetadataRecord) v);

        // 3. Aggregate DataRecords using Session Windows
        KTable<Windowed<String>, BatchRecord> dataTable = dataStream
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(20),Duration.ofSeconds(20)))
                .aggregate(
                        () -> new BatchRecord(new ArrayList<>(),null),
                        (key, value, aggregate) -> {
                            aggregate.getDataRecordList().add(value);
                            return aggregate;
                        },
                        (key, left, right) -> {
                            left.getDataRecordList().addAll(right.getDataRecordList());
                            left.setMetadataRecord(right.getMetadataRecord());
                            return left;
                        },
                        Materialized.<String, BatchRecord, SessionStore<Bytes, byte[]>>as(DATA_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(batchRecordSerde)
                                .withRetention(Duration.ofMinutes(10))
                );

        // 4. Trigger external API call when metadata arrives
        metadataStream.process(
                () -> new MetadataDrivenApiProcessor(DATA_STORE),
                DATA_STORE
        );

        return builder.build();
    }
}

