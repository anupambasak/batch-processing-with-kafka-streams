package com.anupambasak.spring.config;

import com.anupambasak.dtos.BaseRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.boot.context.properties.source.InvalidConfigurationPropertyValueException;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.kafka.autoconfigure.SslBundleSslEngineFactory;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

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

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs(Environment environment,
                                                     KafkaProperties kafkaProperties,
                                                     KafkaConnectionDetails connectionDetails,
                                                     Serde<BaseRecord> baseRecordSerde) {

        Map<String, Object> properties = kafkaProperties.buildStreamsProperties();
//        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, baseRecordSerde.getClass().getName());
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());


//        applyKafkaConnectionDetailsForStreams(properties, connectionDetails);
        KafkaConnectionDetails.Configuration streams = connectionDetails.getStreams();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, streams.getBootstrapServers());
//        KafkaAutoConfiguration.applySecurityProtocol(properties, streams.getSecurityProtocol());
        if (StringUtils.hasLength(streams.getSecurityProtocol())) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, streams.getSecurityProtocol());
        }
//        KafkaAutoConfiguration.applySslBundle(properties, streams.getSslBundle());
        if (streams.getSslBundle() != null) {
            properties.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, SslBundleSslEngineFactory.class);
            properties.put(SslBundle.class.getName(), streams.getSslBundle());
        }

        if (kafkaProperties.getStreams().getApplicationId() == null) {
            String applicationName = environment.getProperty("spring.application.name");
            if (applicationName == null) {
                throw new InvalidConfigurationPropertyValueException("spring.kafka.streams.application-id", null,
                        "This property is mandatory and fallback 'spring.application.name' is not set either.");
            }
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);
        }
        return new KafkaStreamsConfiguration(properties);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return fb -> fb.setStateListener((newState, oldState) -> {
            System.out.println("State transition from " + oldState + " to " + newState);
        });
    }

    @Bean
    public KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        final KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService =
                new KafkaStreamsInteractiveQueryService(streamsBuilderFactoryBean);
        return kafkaStreamsInteractiveQueryService;
    }
}
