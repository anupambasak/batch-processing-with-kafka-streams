package com.anupambasak.controller;

import com.anupambasak.proto.DataRecord;
import com.anupambasak.proto.MetadataRecord;
import com.anupambasak.proto.ProtoBufRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@Slf4j
public class ProtobufDataProducerController {

    private static final String INPUT_TOPIC = "protoMessageTopic";

    @Autowired
    KafkaTemplate<String, ProtoBufRecord> protoBufMessageKafkaTemplate;

    @GetMapping("/produce/protobuf/{producerId}/{count}")
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
            DataRecord dataRecord = DataRecord.newBuilder()
                    .setProducerId(producerId)
                    .setPayload("Data " + i)
                    .build();
            ProtoBufRecord pbr = ProtoBufRecord.newBuilder()
                    .setData(dataRecord)
                    .build();
            protoBufMessageKafkaTemplate.send(INPUT_TOPIC, producerId, pbr);
        }
    }

    private void sendMetaData(String producerId, int count) {
        long currentTime = System.currentTimeMillis();
        MetadataRecord metadataRecord = MetadataRecord.newBuilder()
                .setCreationTimestamp(currentTime)
                .setProducerId(producerId)
                .setTotalRecords(count)
                .build();
        ProtoBufRecord pbr = ProtoBufRecord.newBuilder()
                .setMetadata(metadataRecord)
                .build();
        protoBufMessageKafkaTemplate.send(INPUT_TOPIC, producerId, pbr); // Send with producerId as key and header
        log.info("Producing metadata for producerId: {} at {}", producerId, currentTime);
    }
}