package com.cloudera.fce.envelope.planner;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

public class PlannedRecord extends Record {
    
    private OperationType operationType;

    public PlannedRecord(GenericRecord existing, OperationType operationType) {
        super((Record)existing, true);
        setOperationType(operationType);
    }
    
    public OperationType getOperationType() {
        return operationType;
    }
    
    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }
    
}
