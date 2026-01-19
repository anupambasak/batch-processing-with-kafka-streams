package com.anupambasak;

import com.anupambasak.dtos.DataRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Service
@Slf4j
public class InteractiveQueryService {

    private final KafkaStreamsInteractiveQueryService iqService;
    private final RestTemplate restTemplate;

    public InteractiveQueryService(KafkaStreamsInteractiveQueryService iqService) {
        this.iqService = iqService;
        this.restTemplate = new RestTemplate();
    }

    public List<?> getDistributedData(String producerId) {
        String storeName = "data-store";

        // 1. Find which host has the data for this specific producerId
        HostInfo hostInfo = iqService.getKafkaStreamsApplicationHostInfo(storeName, producerId,
                Serdes.String().serializer());

        log.info("{}", hostInfo);

        // 2. Check if the host is "this" instance
        log.info("{}",iqService.getCurrentKafkaStreamsApplicationHostInfo());
        if (iqService.getCurrentKafkaStreamsApplicationHostInfo().equals(hostInfo)) {
            // Query local state store
            log.info("Reading from local store");
            ReadOnlyKeyValueStore<String, List<DataRecord>> store = iqService.retrieveQueryableStore(
                    storeName, QueryableStoreTypes.keyValueStore());
            return store.get(producerId);
        } else {
            // 3. Remote call: Forward the request to the correct instance
            String remoteUrl = String.format("http://%s:%d/records/%s",
                    hostInfo.host(), hostInfo.port(), producerId);
            log.info("Reading from remote store: {}", remoteUrl);
            return restTemplate.getForObject(remoteUrl, List.class);
        }
    }
}
