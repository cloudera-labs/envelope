package com.cloudera.fce.envelope.planner;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;

import com.cloudera.fce.envelope.RecordModel;
import com.google.common.collect.Lists;

public class InsertOnlyPlanner extends Planner {
    
    public InsertOnlyPlanner(Properties props) throws Exception {
        super(props);
    }

    @Override
    public List<PlannedRecord> planOperations(List<GenericRecord> arrivingRecords,
            List<GenericRecord> existingRecords, RecordModel recordModel) throws Exception
    {
        List<PlannedRecord> planned = Lists.newArrayList();
        
        for (GenericRecord arriving : arrivingRecords) {
            arriving.put(recordModel.getKeyFieldNames().get(0), UUID.randomUUID().toString());
            
            planned.add(new PlannedRecord(arriving, OperationType.INSERT));
        }
        
        return planned;
    }
    
}
