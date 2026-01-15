package com.anupambasak.dtos;

import lombok.Data;

@Data
public class MetadataRecord implements BaseRecord {
    private String producerId;
    private int totalRecords;
}
