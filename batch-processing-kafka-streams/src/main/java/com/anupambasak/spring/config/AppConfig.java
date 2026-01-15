package com.anupambasak.spring.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.CleanupConfig;

@Configuration
@ComponentScan(basePackages = {"com.anupambasak"})
public class AppConfig {

    @Value("${app.kafka.streams.cleanup-on-start:false}")
    private boolean cleanupOnStart;

    @Value("${app.kafka.streams.cleanup-on-stop:false}")
    private boolean cleanupOnStop;

    @Bean
    public CleanupConfig cleanupConfig() {
        return new CleanupConfig(cleanupOnStart, cleanupOnStop);
    }
}
