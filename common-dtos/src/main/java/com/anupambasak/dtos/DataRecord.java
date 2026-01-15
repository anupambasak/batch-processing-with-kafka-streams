package com.anupambasak.dtos;

import lombok.Data;

@Data
public class DataRecord implements BaseRecord {
    private String producerId;
    private String payload;
}