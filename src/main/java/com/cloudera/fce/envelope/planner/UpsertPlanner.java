package com.cloudera.fce.envelope.planner;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.fce.envelope.RecordModel;
import com.cloudera.fce.envelope.utils.RecordUtils;
import com.google.common.collect.Lists;

public class UpsertPlanner extends Planner {

    public UpsertPlanner(Properties props) throws Exception {
        super(props);
    }
    
    @Override
    public List<PlannedRecord> planOperations(List<GenericRecord> arrivingRecords,
            List<GenericRecord> existingRecords, RecordModel recordModel) throws Exception
    {
        
        List<String> keyFieldNames = recordModel.getKeyFieldNames();
        String timestampFieldName = recordModel.getTimestampFieldName();
        List<String> valueFieldNames = recordModel.getValueFieldNames();
        String lastUpdatedFieldName = recordModel.getLastUpdatedFieldName();
        
        Comparator<GenericRecord> tc = new RecordUtils.TimestampComparator(timestampFieldName);
        
        Map<GenericRecord, List<GenericRecord>> arrivedByKey = RecordUtils.recordsByKey(arrivingRecords, keyFieldNames);
        Map<GenericRecord, List<GenericRecord>> existingByKey = RecordUtils.recordsByKey(existingRecords, keyFieldNames);
        
        List<PlannedRecord> planned = Lists.newArrayList();
        
        for (Map.Entry<GenericRecord, List<GenericRecord>> arrivingByKey : arrivedByKey.entrySet()) {
            GenericRecord key = arrivingByKey.getKey();
            List<GenericRecord> arrivingForKey = arrivingByKey.getValue();
            List<GenericRecord> existingForKey = existingByKey.get(key);
            
            if (arrivingForKey.size() > 1) {
                Collections.sort(arrivingForKey, Collections.reverseOrder(tc));
            }
            GenericRecord arrived = arrivingForKey.get(0);
            
            GenericRecord existing = null;
            if (existingForKey != null) {
                existing = existingForKey.get(0);
            }
            
            if (existing == null) {
                arrived.put(lastUpdatedFieldName, currentTimestampString());
                planned.add(new PlannedRecord(arrived, OperationType.INSERT));
            }
            else if (RecordUtils.before(arrived, existing, timestampFieldName)) {
                // We do nothing because the arriving record is older than the existing record
            }
            else if ((RecordUtils.simultaneous(arrived, existing, timestampFieldName) ||
                      RecordUtils.after(arrived, existing, timestampFieldName)) &&
                      RecordUtils.different(arrived, existing, valueFieldNames))
            {
                arrived.put(lastUpdatedFieldName, currentTimestampString());
                planned.add(new PlannedRecord(arrived, OperationType.UPDATE));
            }
        }
        
        return planned;
        
    }

    @Override
    public boolean requiresExisting() {
        return true;
    }

}
