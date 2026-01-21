package com.anupambasak.processor;

import com.anupambasak.dtos.DataRecord;
import com.anupambasak.dtos.MetadataRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import java.time.Duration;
import java.util.List;

@Slf4j
public class MetadataDrivenApiProcessor implements Processor<String, MetadataRecord, Void, Void> {

    private final String storeName;
    private SessionStore<String, List<DataRecord>> store;

    public MetadataDrivenApiProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        Processor.super.init(context);
        this.store = context.getStateStore(storeName);

//        // Schedule a punctuation every 1 minute to check for orphaned data
//        context.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, timestamp -> {
//            try (KeyValueIterator<Windowed<String>, List<DataRecord>> allEntries = store.all()) {
//                while (allEntries.hasNext()) {
//                    KeyValue<Windowed<String>, List<DataRecord>> entry = allEntries.next();
//                    // Logic: If entry is older than a threshold and no metadata arrived,
//                    // handle as a partial/failed EOS or just purge.
//                    if (isStale(entry, timestamp)) {
//                        log.warn("Expiring stale session for key: {}", entry.key.key());
//                        store.remove(entry.key);
//                    }
//                }
//            }
//        });
    }

    @Override
    public void process(Record<String, MetadataRecord> record) {

        String producerId = record.key();
        MetadataRecord metadata = record.value();

        try (KeyValueIterator<Windowed<String>, List<DataRecord>> iterator = store.fetch(producerId)) {

            if (!iterator.hasNext()) {
                log.warn("No data found for producerId {}", producerId);
                return;
            }

            while (iterator.hasNext()) {
                KeyValue<Windowed<String>, List<DataRecord>> entry = iterator.next();

                List<DataRecord> records = entry.value;
                int expected = metadata.getTotalRecords();
                int actual = records.size();

                if (actual == expected) {
                    log.info("✅ Complete batch for producerId={} | records={}", producerId, actual);

                    // 🚀 External API call
                    callExternalApi(producerId, records);
                    // Clean up the store so we don't process this session again on a metadata retry
                    store.remove(entry.key);

                } else {
                    log.warn("❌ Incomplete batch for producerId={} | expected={} actual={}", producerId, expected, actual);
                }
            }
        }
    }

    private void callExternalApi(String producerId, List<DataRecord> records) {
        // REST / gRPC / SOAP / etc
        // This is safe: list is detached from state store
        log.info("callExternalApi producerId={} | records={}", producerId, records.size());
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}

