package com.cloudera.fce.envelope.planner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
        Planner ap = new AppendPlanner(new Properties());
        
        List<GenericRecord> records = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            records.add(createTestRecord());
        }
        
        RecordModel rm = new RecordModel();
        
        List<PlannedRecord> planned = ap.planOperations(records, rm);
        
        for (PlannedRecord plan : planned) {
            assertEquals(plan.getOperationType(), OperationType.INSERT);
        }
        
        assertEquals(planned.size(), 10);
    }
    
    @Test
    public void testUUIDKey() {
        Properties props = new Properties();
        props.setProperty("uuid.key.enabled", "true");
        Planner ap = new AppendPlanner(props);
        
        List<GenericRecord> records = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            records.add(createTestRecord());
        }
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        
        List<PlannedRecord> planned = ap.planOperations(records, rm);
        
        for (PlannedRecord plan : planned) {
            assertNotNull(plan.get("key"));
        }
    }
    
    @Test
    public void testNoUUIDKey() {
        Properties props = new Properties();
        props.setProperty("uuid.key.enabled", "false");
        Planner ap = new AppendPlanner(props);
        
        List<GenericRecord> records = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            records.add(createTestRecord());
        }
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        
        List<PlannedRecord> planned = ap.planOperations(records, rm);
        
        for (PlannedRecord plan : planned) {
            assertNull(plan.get("key"));
        }
    }
    
    @Test
    public void testLastUpdated() {
        Properties props = new Properties();
        Planner ap = new AppendPlanner(props);
        
        List<GenericRecord> records = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            records.add(createTestRecord());
        }
        
        RecordModel rm = new RecordModel();
        rm.setLastUpdatedFieldName("lastupdated");
        
        List<PlannedRecord> planned = ap.planOperations(records, rm);
        
        for (PlannedRecord plan : planned) {
            assertNotNull(plan.get("lastupdated"));
        }
    }
    
    @Test
    public void testNoLastUpdated() {
        Properties props = new Properties();
        Planner ap = new AppendPlanner(props);
        
        List<GenericRecord> records = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            records.add(createTestRecord());
        }
        
        RecordModel rm = new RecordModel();
        
        List<PlannedRecord> planned = ap.planOperations(records, rm);
        
        for (PlannedRecord plan : planned) {
            assertNull(plan.get("lastupdated"));
        }
    }
    
    private GenericRecord createTestRecord() {
        Schema schema = SchemaBuilder.builder().record("test").fields()
                .optionalString("key")
                .optionalInt("value")
                .optionalString("lastupdated")
                .endRecord();
        
        GenericRecord record = new GenericData.Record(schema);
        
        record.put("value", 1);
        
        return record;
    }
    
}
