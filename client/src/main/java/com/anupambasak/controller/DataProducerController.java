package com.anupambasak.controller;

import com.anupambasak.dtos.BaseRecord;
import com.anupambasak.dtos.DataRecord;
import com.anupambasak.dtos.MetadataRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
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

    //    private static final String INPUT_TOPIC = "jsonMessageTopicV1";
    private static final String INPUT_TOPIC = "jsonMessageTopic";

    @Autowired
    KafkaTemplate<String, BaseRecord> jsonMessageKafkaTemplate;

    @GetMapping("/produce/{producerId}/{count}")
    public Mono<ResponseEntity<String>> produceData(@PathVariable String producerId, @PathVariable int count) {
        return Mono.fromRunnable(() -> {
                    sendData(producerId, count);
                    sendMetaData(producerId, count);
                }).thenReturn(ResponseEntity.ok("Data produced successfully for producer: " + producerId))
                .doOnError(ex -> log.error("Error producing data for producerId: {}", producerId, ex))
                .onErrorReturn(ResponseEntity.status(500).body("Error producing data."));
    }

    private void sendData(String producerId, int count) {
        log.info("Producing data for producerId: {}", producerId);
        for (int i = 0; i < count; i++) {
            DataRecord dataRecord = new DataRecord();
            dataRecord.setProducerId(producerId);
            dataRecord.setPayload("Data " + i);
            jsonMessageKafkaTemplate.send(INPUT_TOPIC, producerId, dataRecord);
        }
    }

    private void sendMetaData(String producerId, int count) {
        MetadataRecord metadataRecord = new MetadataRecord();
        metadataRecord.setProducerId(producerId);
        metadataRecord.setTotalRecords(count);
        long currentTime = System.currentTimeMillis();
        metadataRecord.setCreationTimestamp(currentTime);
        jsonMessageKafkaTemplate.send(INPUT_TOPIC, producerId, metadataRecord); // Send with producerId as key and header
        log.info("Producing metadata for producerId: {} at {}", producerId, currentTime);
    }
}