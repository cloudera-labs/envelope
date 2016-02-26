package com.cloudera.fce.envelope.storage;

import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.fce.envelope.planner.OperationType;
import com.cloudera.fce.envelope.planner.PlannedRecord;

public abstract class StorageTable {
    
    public abstract Set<OperationType> getSupportedOperationTypes();
    public abstract Schema getSchema();
    
    public abstract List<GenericRecord> getExistingForFilter(GenericRecord filter) throws Exception;
    
    public abstract void applyPlannedOperations(List<PlannedRecord> operations) throws Exception;
    
}
