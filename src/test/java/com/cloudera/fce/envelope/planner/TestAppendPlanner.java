package com.cloudera.fce.envelope.planner;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import com.cloudera.fce.envelope.RecordModel;
import com.google.common.collect.Lists;

public class TestAppendPlanner {

    @Test
    public void testPlansInserts() {
        AppendPlanner ap = new AppendPlanner(new Properties());
        
        List<GenericRecord> records = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            records.add(createTestRecord());
        }
        
        RecordModel rm = new RecordModel();
        
        List<PlannedRecord> planned = ap.planOperations(records, null, rm);
        
        for (PlannedRecord plan : planned) {
            assertTrue(plan.getOperationType().equals(OperationType.INSERT));
        }
    }
    
    @Test
    public void testUUIDKey() {
        Properties props = new Properties();
        props.setProperty("uuid.key.enabled", "true");
        AppendPlanner ap = new AppendPlanner(props);
        
        List<GenericRecord> records = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            records.add(createTestRecord());
        }
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        
        List<PlannedRecord> planned = ap.planOperations(records, null, rm);
        
        for (PlannedRecord plan : planned) {
            assertNotNull(plan.get("key"));
        }
    }
    
    @Test
    public void testNoUUIDKey() {
        Properties props = new Properties();
        props.setProperty("uuid.key.enabled", "false");
        AppendPlanner ap = new AppendPlanner(props);
        
        List<GenericRecord> records = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            records.add(createTestRecord());
        }
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        
        List<PlannedRecord> planned = ap.planOperations(records, null, rm);
        
        for (PlannedRecord plan : planned) {
            assertNull(plan.get("key"));
        }
    }
    
    @Test
    public void testLastUpdated() {
        Properties props = new Properties();
        AppendPlanner ap = new AppendPlanner(props);
        
        List<GenericRecord> records = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            records.add(createTestRecord());
        }
        
        RecordModel rm = new RecordModel();
        rm.setLastUpdatedFieldName("lastupdated");
        
        List<PlannedRecord> planned = ap.planOperations(records, null, rm);
        
        for (PlannedRecord plan : planned) {
            assertNotNull(plan.get("lastupdated"));
        }
    }
    
    @Test
    public void testNoLastUpdated() {
        Properties props = new Properties();
        AppendPlanner ap = new AppendPlanner(props);
        
        List<GenericRecord> records = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            records.add(createTestRecord());
        }
        
        RecordModel rm = new RecordModel();
        
        List<PlannedRecord> planned = ap.planOperations(records, null, rm);
        
        for (PlannedRecord plan : planned) {
            assertNull(plan.get("lastupdated"));
        }
    }
    
    private GenericRecord createTestRecord() {
        Schema schema = SchemaBuilder.builder().record("a").fields()
                .optionalString("key")
                .optionalInt("value")
                .optionalString("lastupdated")
                .endRecord();
        
        GenericRecord record = new GenericData.Record(schema);
        
        record.put("value", 1);
        
        return record;
    }
    
}
