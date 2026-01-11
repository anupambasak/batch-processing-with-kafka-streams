package com.anupambasak.processor;

import com.anupambasak.dtos.DataRecord;
import com.anupambasak.dtos.MetadataRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.core.type.TypeReference;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Configuration
@Slf4j
public class RecordProcessor {

    @Bean
    public Consumer<KStream<String, Object>> recordUpdator() {
        return input -> {
            final Serde<DataRecord> dataRecordSerde = new JacksonJsonSerde<>(DataRecord.class);
            final Serde<List<DataRecord>> listDataRecordSerde = new JacksonJsonSerde<>(new TypeReference<List<DataRecord>>() {});

            KStream<String, DataRecord> dataRecordsStream = input
                    .filter((k, v) -> {
                        if(v instanceof DataRecord){
                            return true;
                        }else{
                            return false;
                        }
                    })
                    .mapValues(v -> (DataRecord) v);

            KTable<String, List<DataRecord>> dataRecordsTable = dataRecordsStream
                    .groupByKey(Grouped.with(Serdes.String(), dataRecordSerde))
                    .aggregate(
                            ArrayList::new,
                            (key, value, aggregate) -> {
                                aggregate.add(value);
                                return aggregate;
                            },
                            Materialized.with(Serdes.String(), listDataRecordSerde)
                    );

            KStream<String, MetadataRecord> metadataRecordsStream = input
                    .filter((k, v) -> v instanceof MetadataRecord)
                    .mapValues(v -> (MetadataRecord) v);

            metadataRecordsStream.join(dataRecordsTable,
                    (metadata, data) -> {
                        log.info("Processing records for producer: {}. Found {} records. Expecting {}", metadata.getProducerId(), data.size(), metadata.getTotalRecords());
                        if (metadata.getTotalRecords() == data.size()) {
                            log.info("All records received for producer: {}. Processing now.", metadata.getProducerId());
                            // process all records together
                        } else {
                            log.warn("Record count mismatch for producer: {}. Expected: {}, Actual: {}", metadata.getProducerId(), metadata.getTotalRecords(), data.size());
                        }
                        return "";
                    });
        };
    }
}
