package com.anupambasak.spring.config;

import com.anupambasak.dtos.BaseRecord;
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

    @Bean("jsonMessageProducerFactory")
    public ProducerFactory<String, BaseRecord> jsonMessageProducerFactory(){
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        ProducerFactory<String, BaseRecord> producerFactory = new DefaultKafkaProducerFactory<>(config);
        return producerFactory;
    }

    @Bean("jsonMessageKafkaTemplate")
    public KafkaTemplate<String, BaseRecord> jsonMessageKafkaTemplate(ProducerFactory<String, BaseRecord> jsonMessageProducerFactory){
        KafkaTemplate<String, BaseRecord> kafkaTemplate = new KafkaTemplate<>(jsonMessageProducerFactory);
        return kafkaTemplate;
    }
}
