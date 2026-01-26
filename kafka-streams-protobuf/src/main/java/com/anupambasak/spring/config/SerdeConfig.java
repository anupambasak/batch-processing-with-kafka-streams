package com.anupambasak.spring.config;

import com.anupambasak.proto.BatchRecord;
import com.anupambasak.proto.MetadataRecord;
import com.anupambasak.proto.ProtoBufRecord;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class SerdeConfig {

    private final Map<String, Object> commonConfig;

    public SerdeConfig(KafkaProperties kafkaProperties) {
        this.commonConfig = new HashMap<>(kafkaProperties.getProperties());
    }

    @Bean
    public KafkaProtobufSerde<ProtoBufRecord> protoBufRecordSerde() {
        KafkaProtobufSerde<ProtoBufRecord> serde = new KafkaProtobufSerde<>();
        Map<String, Object> config = new HashMap<>(commonConfig);
        config.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, ProtoBufRecord.class);
        serde.configure(config, false);
        return serde;
    }

    @Bean
    public KafkaProtobufSerde<BatchRecord> batchRecordSerde() {
        KafkaProtobufSerde<BatchRecord> serde = new KafkaProtobufSerde<>();
        Map<String, Object> config = new HashMap<>(commonConfig);
        config.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, BatchRecord.class);
        serde.configure(config, false);
        return serde;
    }

    @Bean
    public KafkaProtobufSerde<MetadataRecord> metadataRecordSerde() {
        KafkaProtobufSerde<MetadataRecord> serde = new KafkaProtobufSerde<>();
        Map<String, Object> config = new HashMap<>(commonConfig);
        config.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, MetadataRecord.class);
        serde.configure(config, false);
        return serde;
    }
}
