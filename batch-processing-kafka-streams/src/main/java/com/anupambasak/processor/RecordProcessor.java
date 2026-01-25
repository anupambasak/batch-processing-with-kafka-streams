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

    private static final String INPUT_TOPIC = "jsonMessageTopic";
    private static final String DATA_STORE = "data-store";

    @Autowired
    private Serde<BaseRecord> baseRecordSerde;

    @Autowired
    private Serde<MetadataRecord> metadataRecordSerde;

    @Autowired
    private Serde<BatchRecord> batchRecordSerde;


    @Bean
    public KStream<String, BatchRecord> kStream(StreamsBuilder streamsBuilder) {

        // 1. Consume from input topic
        KStream<String, BaseRecord> input = streamsBuilder.stream(INPUT_TOPIC,
                Consumed.with(Serdes.String(), baseRecordSerde));

        // 2. Split into two streams
        Map<String, KStream<String, BaseRecord>> branches = input.split(Named.as("branch-"))
                .branch((k, v) -> v instanceof DataRecord, Branched.as("data"))
                .branch((k, v) -> v instanceof MetadataRecord, Branched.as("metadata"))
                .noDefaultBranch();

        // 3. Aggregate DataRecords into session windows to isolate batches
        KTable<Windowed<String>, BatchRecord> dataTable = branches.get("branch-data")
                .mapValues(v -> (DataRecord) v)
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(2),Duration.ofSeconds(10)))
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

        KStream<String, MetadataRecord> metadataStream = branches.get("branch-metadata")
                .mapValues(v -> (MetadataRecord) v)
                .filter((k, v) -> k != null && v != null);

        KStream<String, BatchRecord> dataTableSeream = dataTable.toStream()
                .selectKey((k, v) -> k.key())
                .filter((k, v) -> k != null && v != null);

        // 4. Join the aggregated data's changelog stream with MetadataRecord stream
        return metadataStream
                .join(
                        dataTableSeream,
                        (metadata, data) -> {
                            if (data != null && data.getDataRecordList().size() == metadata.getTotalRecords()) {
                                log.info("✅ Complete batch for producerId={} | records={}",
                                        metadata.getProducerId(), data.getDataRecordList().size());
                                data.setMetadataRecord(metadata);
                                // 🚀 External API call
                                callExternalApi(data);
                            }
                            return data;
                        },
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(4)),
                        StreamJoined.with(Serdes.String(), metadataRecordSerde, batchRecordSerde)
                );
    }

    private void callExternalApi(BatchRecord batchRecord) {
        // REST / gRPC / SOAP / etc
        // This is safe: list is detached from state store
        final long processingTime = (System.currentTimeMillis() - batchRecord.getMetadataRecord().getCreationTimestamp());
        final String producerId = batchRecord.getMetadataRecord().getProducerId();
        final List<DataRecord> dataRecordList = batchRecord.getDataRecordList();
        log.info("✅ callExternalApi producerId={} | records={} | timeTaken={}ms", producerId, dataRecordList.size(), processingTime);
    }
}