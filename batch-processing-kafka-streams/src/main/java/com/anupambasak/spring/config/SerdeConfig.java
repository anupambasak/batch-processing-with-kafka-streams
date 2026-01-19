package com.anupambasak.spring.config;

import com.anupambasak.dtos.BaseRecord;
import com.anupambasak.dtos.DataRecord;
import com.anupambasak.dtos.MetadataRecord;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DefaultTyping;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import tools.jackson.databind.jsontype.PolymorphicTypeValidator;

import java.util.List;

@Configuration
public class SerdeConfig {

    @Bean
    public JsonMapper jsonMapper() {
        // 1. Create the Validator
        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(BaseRecord.class) // Safety: only allow specific base types
                .allowIfBaseType(java.util.List.class)
                .allowIfSubType("com.anupambasak.*")
                .build();

        // 2. Use the Builder to create an immutable Mapper
        JsonMapper mapper = JsonMapper.builder()
                .polymorphicTypeValidator(ptv)
                .activateDefaultTyping(
                        ptv,
                        DefaultTyping.NON_FINAL,
                        JsonTypeInfo.As.PROPERTY
                )
                .build();
        return mapper;
    }

    @Bean
    public Serde<BaseRecord> baseRecordSerde(JsonMapper jsonMapper) {
        JacksonJsonSerializer<BaseRecord> serializer = new JacksonJsonSerializer<>(jsonMapper);
        serializer.setAddTypeInfo(true);

        JacksonJsonDeserializer<BaseRecord> deserializer = new JacksonJsonDeserializer<>(BaseRecord.class,jsonMapper);
        deserializer.setUseTypeHeaders(true);
        deserializer.addTrustedPackages("com.anupambasak.dtos");
        return Serdes.serdeFrom(serializer, deserializer);
    }

    @Bean
    public Serde<DataRecord> dataRecordSerde(JsonMapper  jsonMapper) {
        JacksonJsonSerializer<DataRecord> serializer = new JacksonJsonSerializer<>(jsonMapper);
        serializer.setAddTypeInfo(true);

        JacksonJsonDeserializer<DataRecord> deserializer = new JacksonJsonDeserializer<>(DataRecord.class,jsonMapper);
        deserializer.setUseTypeHeaders(true);
        deserializer.addTrustedPackages("com.anupambasak.dtos");

        return Serdes.serdeFrom(serializer, deserializer);
    }

    @Bean
    public Serde<MetadataRecord> metadataRecordSerde(JsonMapper  jsonMapper) {
        JacksonJsonSerializer<MetadataRecord> serializer = new JacksonJsonSerializer<>(jsonMapper);
        serializer.setAddTypeInfo(true);

        JacksonJsonDeserializer<MetadataRecord> deserializer = new JacksonJsonDeserializer<>(MetadataRecord.class,jsonMapper);
        deserializer.setUseTypeHeaders(true);
        deserializer.addTrustedPackages("com.anupambasak.dtos");

        return Serdes.serdeFrom(serializer, deserializer);
    }

    @Bean
    public Serde<List<DataRecord>> dataRecordListSerde(JsonMapper  jsonMapper) {

        JacksonJsonSerializer<List<DataRecord>> serializer = new JacksonJsonSerializer<>(jsonMapper);
        serializer.setAddTypeInfo(true);

        JacksonJsonDeserializer<List<DataRecord>> deserializer = new JacksonJsonDeserializer<>(new TypeReference<List<DataRecord>>() {},jsonMapper);
        deserializer.setUseTypeHeaders(true);
        deserializer.addTrustedPackages("com.anupambasak.dtos");

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
