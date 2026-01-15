package com.anupambasak.controller;

import com.anupambasak.dtos.DataRecord;
import com.anupambasak.dtos.MetadataRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@Slf4j
public class DataProducerController {

    private final StreamBridge streamBridge;

    @GetMapping("/produce/{producerId}")
    public Mono<ResponseEntity<String>> produceData(@PathVariable String producerId) {
        // Correct binding name for the Kafka Streams processor
        final String bindingName = "dataProducer-out-0";

        return Mono.fromRunnable(() -> {
                    log.info("Producing data for producerId: {}", producerId);
                    // Send 10 DataRecords as per original requirement
                    for (int i = 0; i < 10; i++) {
                        DataRecord dataRecord = new DataRecord();
                        dataRecord.setProducerId(producerId);
                        dataRecord.setPayload("Data " + i);

                        Message<DataRecord> message = MessageBuilder
                                .withPayload(dataRecord)
                                .setHeader(KafkaHeaders.KEY, producerId)
                                .setHeader("type", DataRecord.class.getName())
                                .build();
                        streamBridge.send(bindingName, message); // Send with producerId as key
                    }

                    // Send the MetadataRecord after all DataRecords
                    MetadataRecord metadataRecord = new MetadataRecord();
                    metadataRecord.setProducerId(producerId);
                    metadataRecord.setTotalRecords(10); // Set total records to 10
                    Message<MetadataRecord> metadataMessage = MessageBuilder
                            .withPayload(metadataRecord)
                            .setHeader("recordType", "metadata")
                            .setHeader(KafkaHeaders.KEY, producerId)
                            .setHeader("type", MetadataRecord.class.getName())
                            .build();
                    streamBridge.send(bindingName, metadataMessage); // Send with producerId as key and header
                    log.info("Finished producing data for producerId: {}", producerId);
                }).thenReturn(ResponseEntity.ok("Data produced successfully for producer: " + producerId))
                .doOnError(ex -> log.error("Error producing data for producerId: {}", producerId, ex))
                .onErrorReturn(ResponseEntity.status(500).body("Error producing data."));
    }
}