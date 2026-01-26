package com.anupambasak;

import com.anupambasak.proto.BatchRecord;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StateQueryController {

    private final InteractiveQueryService queryService;

    public StateQueryController(InteractiveQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/data/{producerId}")
    public BatchRecord getRecords(@PathVariable String producerId) {
        BatchRecord data = queryService.getDistributedData(producerId);
        return data != null ? data : BatchRecord.getDefaultInstance();
    }
}

