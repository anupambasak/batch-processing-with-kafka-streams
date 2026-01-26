package com.anupambasak.processor;

import com.anupambasak.proto.BatchRecord;
import com.anupambasak.proto.DataRecord;
import com.anupambasak.proto.MetadataRecord;
import com.anupambasak.proto.ProtoBufRecord;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Configuration
@Slf4j
public class ProtoBufRecordProcessor {

    private static final String INPUT_TOPIC = "protoMessageTopic";
    private static final String DATA_STORE = "protobuf-data-store";


    @Bean
    public KStream<String, MetadataRecord> kProtoBufStream(
            @Qualifier("protoBufStreamsBuilder")
            StreamsBuilder streamsBuilder,
            KafkaProtobufSerde<ProtoBufRecord> kafkaProtobufSerde,
            KafkaProtobufSerde<BatchRecord> batchRecordSerde,
            KafkaProtobufSerde<MetadataRecord> metadataRecordSerde) {

        // 1. Consume from input topic
        KStream<String, ProtoBufRecord> input = streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), kafkaProtobufSerde));

        // 2. Split into two streams
        Map<String, KStream<String, ProtoBufRecord>> branches = input.split(Named.as("branch-"))
                .branch((k, v) -> v.hasData(), Branched.as("data"))
                .branch((k, v) -> v.hasMetadata(), Branched.as("metadata"))
                .noDefaultBranch();

        // 3. Aggregate DataRecords into session windows to isolate batches
        KTable<Windowed<String>, BatchRecord> dataTable = branches.get("branch-data")
                .mapValues(v -> v.getData())
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(2), Duration.ofSeconds(10)))
                .aggregate(
                        () -> BatchRecord.newBuilder().build(),
                        (key, value, aggregate) -> {
                            return BatchRecord.newBuilder(aggregate)
                                    .addDataRecordList(value)
                                    .build();
                        },
                        (key, left, right) -> {
                            return BatchRecord.newBuilder(left)
                                    .addAllDataRecordList(right.getDataRecordListList())
                                    .setMetadataRecord(right.getMetadataRecord())
                                    .build();
                        },
                        Materialized.<String, BatchRecord, SessionStore<Bytes, byte[]>>as(DATA_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(batchRecordSerde)
                                .withRetention(Duration.ofMinutes(10))
                );

        KStream<String, MetadataRecord> metadataStream = branches.get("branch-metadata")
                .mapValues(v -> v.getMetadata())
                .filter((k, v) -> k != null && v != null);

        KStream<String, BatchRecord> dataTableSeream = dataTable.toStream()
                .selectKey((k, v) -> k.key())
                .filter((k, v) -> k != null && v != null);

        // 4. Join the aggregated data's changelog stream with MetadataRecord stream
        return metadataStream
                .join(
                        dataTableSeream,
                        (metadata, data) -> {
                            if (data != null && data.getDataRecordListCount() == metadata.getTotalRecords()) {
                                log.info("✅ Complete batch for producerId={} | records={}",
                                        metadata.getProducerId(), data.getDataRecordListCount());
                                BatchRecord rData = BatchRecord.newBuilder()
                                        .setMetadataRecord(metadata)
                                        .build();
                                // 🚀 External API call
                                callExternalApi(rData);
                            }
                            return metadata;
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
        final List<DataRecord> dataRecordList = batchRecord.getDataRecordListList();
        log.info("✅ callExternalApi producerId={} | records={} | timeTaken={}ms", producerId, dataRecordList.size(), processingTime);
    }
}