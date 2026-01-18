package com.anupambasak.spring.config;

import com.anupambasak.dtos.BaseRecord;
import com.anupambasak.dtos.DataRecord;
import com.anupambasak.dtos.MetadataRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import java.util.List;

@Configuration
public class SerdeConfig {

    @Bean
    public Serde<BaseRecord> baseRecordSerde() {
        return Serdes.serdeFrom(
                new JacksonJsonSerializer<>(), new JacksonJsonDeserializer<>(BaseRecord.class));
    }

    @Bean
    public Serde<DataRecord> dataRecordSerde() {
        return Serdes.serdeFrom(
                new JacksonJsonSerializer<>(), new JacksonJsonDeserializer<>(DataRecord.class));
    }

    @Bean
    public Serde<MetadataRecord> metadataRecordSerde() {
        return Serdes.serdeFrom(
                new JacksonJsonSerializer<>(), new JacksonJsonDeserializer<>(MetadataRecord.class));
    }

    @Bean
    public Serde<List<DataRecord>> dataRecordListSerde() {
        return Serdes.serdeFrom(
                new JacksonJsonSerializer<>(), new JacksonJsonDeserializer<>(List.class));
    }
}
