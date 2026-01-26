package com.anupambasak.dtos;

import lombok.Data;

@Data
public class ProtoBufRecord {
    private MetadataRecord metadata;
    private DataRecord data;
}
