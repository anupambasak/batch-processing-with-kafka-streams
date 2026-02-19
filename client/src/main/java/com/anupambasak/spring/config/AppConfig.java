package com.anupambasak.spring.config;

import com.anupambasak.dtos.BaseRecord;
import com.anupambasak.proto.ProtoBufRecord;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ComponentScan(basePackages = {"com.anupambasak"})
@Slf4j
public class AppConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public NewTopic jsonMessageTopic() {
        return TopicBuilder.name("jsonMessageTopic")
                .build();
    }

    @Bean
    public NewTopic protoMessageTopic() {
        return TopicBuilder.name("protoMessageTopic")
                .build();
    }

    @Bean
    public NewTopic protoMessageTopicV2() {
        return TopicBuilder.name("protoMessageTopicV2")
                .build();
    }

    @Bean("jsonMessageProducerFactory")
    public ProducerFactory<String, BaseRecord> jsonMessageProducerFactory(){
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS, true);

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean("jsonMessageKafkaTemplate")
    public KafkaTemplate<String, BaseRecord> jsonMessageKafkaTemplate(ProducerFactory<String, BaseRecord> jsonMessageProducerFactory){
        KafkaTemplate<String, BaseRecord> kafkaTemplate = new KafkaTemplate<>(jsonMessageProducerFactory);
        return kafkaTemplate;
    }

    @Bean("protoBufMessageProducerFactory")
    public ProducerFactory<String, ProtoBufRecord> protoBufMessageProducerFactory(){
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put("schema.registry.url", "http://localhost:8081");
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean("protoBufMessageKafkaTemplate")
    public KafkaTemplate<String, ProtoBufRecord> protoBufMessageKafkaTemplate(
            ProducerFactory<String, ProtoBufRecord> protoBufMessageProducerFactory){
        KafkaTemplate<String, ProtoBufRecord> kafkaTemplate = new KafkaTemplate<>(protoBufMessageProducerFactory);
        return kafkaTemplate;
    }

    @Bean("protoBufMessageProducerFactoryWithApicurio")
    public ProducerFactory<String, ProtoBufRecord> protoBufMessageProducerFactoryWithApicurio(){
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtobufKafkaSerializer.class);
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE);
        config.put(SerdeConfig.REGISTRY_URL, "http://localhost:8083");
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean("protoBufMessageKafkaTemplateWithApicurio")
    public KafkaTemplate<String, ProtoBufRecord> protoBufMessageKafkaTemplateWithApicurio(
            ProducerFactory<String, ProtoBufRecord> protoBufMessageProducerFactoryWithApicurio){
        KafkaTemplate<String, ProtoBufRecord> kafkaTemplate = new KafkaTemplate<>(protoBufMessageProducerFactoryWithApicurio);
        return kafkaTemplate;
    }

}
