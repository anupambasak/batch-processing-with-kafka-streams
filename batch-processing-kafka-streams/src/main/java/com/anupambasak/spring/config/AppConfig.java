package com.anupambasak.spring.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;

@Configuration
@ComponentScan(basePackages = {"com.anupambasak"})
@EnableKafkaStreams
public class AppConfig {

    @Bean
    public NewTopic inputTopic() {
        return TopicBuilder.name("jsonMessageTopic")
                .build();
    }

    @Bean
    public CleanupConfig cleanupConfig() {
        // cleanupOnStart = true, cleanupOnStop = true
        return new CleanupConfig(true, true);
    }

    @Bean
    public KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        final KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService =
                new KafkaStreamsInteractiveQueryService(streamsBuilderFactoryBean);
        return kafkaStreamsInteractiveQueryService;
    }
}
