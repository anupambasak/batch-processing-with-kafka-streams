package com.anupambasak;

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

    @GetMapping("/records/{producerId}")
    public List<?> getRecords(@PathVariable String producerId) {
        List<?> data = queryService.getDistributedData(producerId);
        return data != null ? data : List.of();
    }
}
