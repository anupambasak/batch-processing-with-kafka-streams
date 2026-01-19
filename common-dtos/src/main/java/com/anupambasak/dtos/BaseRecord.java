package com.anupambasak.dtos;


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DataRecord.class),
        @JsonSubTypes.Type(value = MetadataRecord.class)
})
public interface BaseRecord {
}
