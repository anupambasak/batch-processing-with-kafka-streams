package com.anupambasak;

import com.anupambasak.dtos.DataRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
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

    public List<DataRecord> getDistributedData(String producerId) {
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
            ReadOnlySessionStore<String, List<DataRecord>> store = iqService.retrieveQueryableStore(
                    storeName, QueryableStoreTypes.sessionStore());

            KeyValueIterator<Windowed<String>, List<DataRecord>> iterator = store.fetch(producerId);
            List<DataRecord> latestSession = new ArrayList<>();
            long latestEndTime = -1;

            while (iterator.hasNext()) {
                KeyValue<Windowed<String>, List<DataRecord>> next = iterator.next();
                if (next.key.window().end() > latestEndTime) {
                    latestEndTime = next.key.window().end();
                    latestSession = next.value;
                }
            }
            iterator.close();
            return latestSession;
        } else {
            // 3. Remote call: Forward the request to the correct instance
            String remoteUrl = String.format("http://%s:%d/data/%s",
                    hostInfo.host(), hostInfo.port(), producerId);
            log.info("Reading from remote store: {}", remoteUrl);
            return restTemplate.getForObject(remoteUrl, List.class);
        }
    }
}
