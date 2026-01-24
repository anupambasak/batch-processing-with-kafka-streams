package com.anupambasak.processor;

import com.anupambasak.dtos.BaseRecord;
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
import java.util.List;

@Configuration
@Slf4j
public class RecordProcessorV1 {

    private static final String INPUT_TOPIC = "jsonMessageTopicV1";
    private static final String DATA_STORE = "data-storeV1";

    @Autowired
    private Serde<BaseRecord> baseRecordSerde;

    @Autowired
    private Serde<List<DataRecord>> dataRecordListSerde;

    @Autowired
    private Serde<MetadataRecord> metadataRecordSerde;

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
        KTable<Windowed<String>, List<DataRecord>> dataTable = dataStream
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(2), Duration.ofSeconds(30)))
                .aggregate(
                        ArrayList::new,
                        // ADD record (immutable-style)
                        (key, value, aggregate) -> {
                            List<DataRecord> updated = new ArrayList<>(aggregate);
                            updated.add(value);
                            return updated;
                        },
                        // MERGE sessions safely
                        (key, left, right) -> {
                            List<DataRecord> merged = new ArrayList<>(left);
                            merged.addAll(right);
                            return merged;
                        },
                        Materialized.<String, List<DataRecord>, SessionStore<Bytes, byte[]>>as(DATA_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(dataRecordListSerde)
                                .withRetention(Duration.ofMinutes(10))
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().withMaxRecords(15000)));

        // 4. Trigger external API call when metadata arrives
        metadataStream.process(
                () -> new MetadataDrivenApiProcessor(DATA_STORE),
                DATA_STORE
        );

        return builder.build();
    }
}

