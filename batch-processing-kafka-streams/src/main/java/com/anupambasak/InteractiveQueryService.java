package com.anupambasak;

import com.anupambasak.dtos.DataRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
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
    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    private static final String DATA_STORE = "data-store";

    public InteractiveQueryService(KafkaStreamsInteractiveQueryService iqService, StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.iqService = iqService;
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
        this.restTemplate = new RestTemplate();
    }

    public List<DataRecord> getDistributedData(String producerId) {
        String storeName = "data-store";

        // 1. Find which host has the data for this specific producerId
        HostInfo hostInfo = iqService.getKafkaStreamsApplicationHostInfo(storeName, producerId,
                Serdes.String().serializer());

        log.info("{}", hostInfo);

        if(streamsBuilderFactoryBean.getKafkaStreams().state() != KafkaStreams.State.RUNNING){
            log.warn("Streams not ready yet, current state: {}", streamsBuilderFactoryBean.getKafkaStreams().state());
            throw new RuntimeException("Streams state:  " + streamsBuilderFactoryBean.getKafkaStreams().state());
        }

        // 2. Check if the host is "this" instance
        log.info("{}",iqService.getCurrentKafkaStreamsApplicationHostInfo());
        if (iqService.getCurrentKafkaStreamsApplicationHostInfo().equals(hostInfo)) {
            // Query local state store
            log.info("Reading from local store");
//            return readSessionStoreData(producerId);
            return readKeyValueStoreData(producerId);
        } else {
            // 3. Remote call: Forward the request to the correct instance
            String remoteUrl = String.format("http://%s:%d/data/%s",
                    hostInfo.host(), hostInfo.port(), producerId);
            log.info("Reading from remote store: {}", remoteUrl);
            return restTemplate.getForObject(remoteUrl, List.class);
        }
    }

    public List<DataRecord> readSessionStoreData(String producerId) {
        ReadOnlySessionStore<String, List<DataRecord>> store = iqService.retrieveQueryableStore(
                DATA_STORE, QueryableStoreTypes.sessionStore());

        long start = System.currentTimeMillis();
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
        long end = System.currentTimeMillis();
        log.info("Time to fetch :{} ms",(end-start));
        return latestSession;
    }

    public List<DataRecord> readKeyValueStoreData(String producerId) {
        ReadOnlyKeyValueStore<String, List<DataRecord>> store = iqService.retrieveQueryableStore(
                DATA_STORE, QueryableStoreTypes.keyValueStore());

        long start = System.currentTimeMillis();
        List<DataRecord> records = store.get(producerId);
        long end = System.currentTimeMillis();
        log.info("Time to fetch :{} ms",(end-start));
        if (records == null) {
            return new ArrayList<>();
        }
        return records;
    }

}
