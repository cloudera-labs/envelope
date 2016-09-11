package com.cloudera.labs.envelope.plan;

import org.apache.spark.sql.Row;

/**
 * A typed record in the storage schema with an attached mutation plan.
 */
public class PlannedRow {
    
    private MutationType mutationType;
    private Row row;

    public PlannedRow(Row row, MutationType mutationType) {
        this.row = row;
        this.mutationType = mutationType;
    }
    
    public MutationType getMutationType() {
        return mutationType;
    }
    
    public Row getRow() {
        return row;
    }
    
    public void setMutationType(MutationType mutationType) {
        this.mutationType = mutationType;
    }
    
    public void setRow(Row row) {
        this.row = row;
    }
    
    @Override
    public String toString() {
        return String.format("Plan: [Mutation: %s], [Row: %s]", mutationType, row.mkString(","));
    }
    
}
