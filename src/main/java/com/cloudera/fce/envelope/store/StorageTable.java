package com.cloudera.fce.envelope.store;

import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.fce.envelope.plan.MutationType;
import com.cloudera.fce.envelope.plan.PlannedRecord;

public abstract class StorageTable {
    
    public abstract Set<MutationType> getSupportedMutationTypes();
    public abstract Schema getSchema();
    
    public abstract List<GenericRecord> getExistingForFilter(GenericRecord filter) throws Exception;
    
    public abstract void applyPlannedMutations(List<PlannedRecord> mutations) throws Exception;
    
}
