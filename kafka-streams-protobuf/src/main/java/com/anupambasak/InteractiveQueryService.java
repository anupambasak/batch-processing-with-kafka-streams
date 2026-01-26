package com.anupambasak;

import com.anupambasak.proto.BatchRecord;
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

@Service
@Slf4j
public class InteractiveQueryService {

    private final KafkaStreamsInteractiveQueryService protoBufIqService;
    private final RestTemplate restTemplate;
    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    private static final String DATA_STORE = "protobuf-data-store";

    public InteractiveQueryService(KafkaStreamsInteractiveQueryService protoBufIqService, StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.protoBufIqService = protoBufIqService;
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
        this.restTemplate = new RestTemplate();
    }

    public BatchRecord getDistributedData(String producerId) {

        // 1. Find which host has the data for this specific producerId
        HostInfo hostInfo = protoBufIqService.getKafkaStreamsApplicationHostInfo(DATA_STORE, producerId,
                Serdes.String().serializer());

        log.info("{}", hostInfo);

        if(streamsBuilderFactoryBean.getKafkaStreams().state() != KafkaStreams.State.RUNNING){
            log.warn("Streams not ready yet, current state: {}", streamsBuilderFactoryBean.getKafkaStreams().state());
            throw new RuntimeException("Streams state:  " + streamsBuilderFactoryBean.getKafkaStreams().state());
        }

        // 2. Check if the host is "this" instance
        log.info("{}",protoBufIqService.getCurrentKafkaStreamsApplicationHostInfo());
        if (protoBufIqService.getCurrentKafkaStreamsApplicationHostInfo().equals(hostInfo)) {
            // Query local state store
            log.info("Reading from local store");
            return readSessionStoreData(producerId);
//            return readKeyValueStoreData(producerId);
        } else {
            // 3. Remote call: Forward the request to the correct instance
            String remoteUrl = String.format("http://%s:%d/data/%s",
                    hostInfo.host(), hostInfo.port(), producerId);
            log.info("Reading from remote store: {}", remoteUrl);
            return restTemplate.getForObject(remoteUrl, BatchRecord.class);
        }
    }

    public BatchRecord readSessionStoreData(String producerId) {
        ReadOnlySessionStore<String, BatchRecord> store = protoBufIqService.retrieveQueryableStore(
                DATA_STORE, QueryableStoreTypes.sessionStore());

        long start = System.currentTimeMillis();
        KeyValueIterator<Windowed<String>, BatchRecord> iterator = store.fetch(producerId);
        BatchRecord latestSession = BatchRecord.newBuilder().build();
        long latestEndTime = -1;

        while (iterator.hasNext()) {
            KeyValue<Windowed<String>, BatchRecord> next = iterator.next();
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

    public BatchRecord readKeyValueStoreData(String producerId) {
        ReadOnlyKeyValueStore<String, BatchRecord> store = protoBufIqService.retrieveQueryableStore(
                DATA_STORE, QueryableStoreTypes.keyValueStore());

        long start = System.currentTimeMillis();
        BatchRecord records = store.get(producerId);
        long end = System.currentTimeMillis();
        log.info("Time to fetch :{} ms",(end-start));
        if (records == null) {
            return BatchRecord.getDefaultInstance();
        }
        return records;
    }

}
