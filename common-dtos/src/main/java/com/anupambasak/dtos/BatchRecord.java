package com.anupambasak.dtos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class BatchRecord {
    private List<DataRecord> dataRecordList;
    private MetadataRecord metadataRecord;
}
