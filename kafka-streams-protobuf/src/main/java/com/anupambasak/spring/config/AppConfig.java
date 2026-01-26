package com.anupambasak.spring.config;

import com.anupambasak.proto.ProtoBufRecord;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;

import java.util.Map;

@Configuration
@ComponentScan(basePackages = {"com.anupambasak"})
@Slf4j
public class AppConfig {

    @Bean
    public NewTopic protoMessageTopic() {
        return TopicBuilder.name("protoMessageTopic")
                .build();
    }

    @Bean
    public CleanupConfig cleanupConfig() {
        return new CleanupConfig(true, true);
    }

    @Bean
    public KafkaStreamsConfiguration protoBufKStreamsConfigs(KafkaProperties kafkaProperties,
                                                             KafkaProtobufSerde<ProtoBufRecord>  protoBufRecordSerde) {

        Map<String, Object> properties = kafkaProperties.buildStreamsProperties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "record-processor-protobuf");
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:7072");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, protoBufRecordSerde.getClass().getName());
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
        return new KafkaStreamsConfiguration(properties);
    }

    // Explicitly define factory bean
    @Bean(name = "protoBufStreamsBuilder")
    public StreamsBuilderFactoryBean protoBufStreamsBuilder(KafkaStreamsConfiguration protoBufKStreamsConfigs,
                                                            ObjectProvider<StreamsBuilderFactoryBeanConfigurer> configurerProvider) {
        StreamsBuilderFactoryBean fb = new StreamsBuilderFactoryBean(protoBufKStreamsConfigs);
        configurerProvider.orderedStream().forEach(configurer -> configurer.configure(fb));
        return fb;
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return factoryBean -> {
            final String streamId = factoryBean.getStreamsConfiguration().getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
            factoryBean.setStateListener((newState, oldState) -> {
                log.info("{} State transition from {} -> {}",streamId, oldState, newState);
            });
        };
    }

    @Bean
    public KafkaStreamsInteractiveQueryService protoBufIqService(
            @Qualifier("protoBufStreamsBuilder") StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        return new KafkaStreamsInteractiveQueryService(streamsBuilderFactoryBean);
    }

}
