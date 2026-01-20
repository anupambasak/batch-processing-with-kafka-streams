package com.anupambasak;

import com.anupambasak.dtos.DataRecord;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class StateQueryController {

    private final InteractiveQueryService queryService;

    public StateQueryController(InteractiveQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/data/{producerId}")
    public List<DataRecord> getRecords(@PathVariable String producerId) {
        List<DataRecord> data = queryService.getDistributedData(producerId);
        return data != null ? data : List.of();
    }
}

