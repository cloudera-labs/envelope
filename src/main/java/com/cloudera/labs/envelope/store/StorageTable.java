package com.cloudera.labs.envelope.store;

import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRecord;

/**
 * Abstract class for storage tables to extend.
 */
public abstract class StorageTable {
    
    /**
     * The mutation types that the storage table is able to apply.
     * @return The set of supported mutation types.
     */
    public abstract Set<MutationType> getSupportedMutationTypes();
    
    /**
     * The schema of the storage table.
     * @return The Apache Avro schema equivalent for the schema of the storage table.
     */
    public abstract Schema getSchema();
    
    /**
     * Get the existing records in the storage table for the given filter.
     * @param filter The record whose field names and values are used as an equality filter on
     * the storage table.
     * @return The list of storage records in the table schema that match the filter.
     */
    public abstract List<GenericRecord> getExistingForFilter(GenericRecord filter) throws Exception;
    
    /**
     * Apply the planned mutations to the storage table.
     * @param mutations The list of planned mutations.
     */
    public abstract void applyPlannedMutations(List<PlannedRecord> mutations) throws Exception;
    
}
