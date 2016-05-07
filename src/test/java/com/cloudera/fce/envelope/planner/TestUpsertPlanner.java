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

public class TestUpsertPlanner {

    @Test
    public void testNotExistingPlansInsert() {
        Planner p = new UpsertPlanner(new Properties());
        
        List<GenericRecord> existing = Lists.newArrayList();
                
        List<GenericRecord> arriving = Lists.newArrayList();
        GenericRecord record = createTestRecord();
        record.put("key", "a");
        record.put("timestamp", 100L);
        record.put("value", "hello");
        arriving.add(record);
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        rm.setTimestampFieldName("timestamp");
        rm.setValueFieldNames(Lists.newArrayList("value"));
        rm.setLastUpdatedFieldName("lastupdated");
        
        List<PlannedRecord> planned = p.planOperations(arriving, existing, rm);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getOperationType(), OperationType.INSERT);
    }
    
    @Test
    public void testEarlierExistingWithNewValuesPlansUpdate() {
        Planner p = new UpsertPlanner(new Properties());
        
        List<GenericRecord> existing = Lists.newArrayList();
        GenericRecord exist = createTestRecord();
        exist.put("key", "a");
        exist.put("timestamp", 50L);
        exist.put("value", "world");
        existing.add(exist);
        
        List<GenericRecord> arriving = Lists.newArrayList();
        GenericRecord arrived = createTestRecord();
        arrived.put("key", "a");
        arrived.put("timestamp", 100L);
        arrived.put("value", "hello");
        arriving.add(arrived);
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        rm.setTimestampFieldName("timestamp");
        rm.setValueFieldNames(Lists.newArrayList("value"));
        rm.setLastUpdatedFieldName("lastupdated");
        
        List<PlannedRecord> planned = p.planOperations(arriving, existing, rm);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getOperationType(), OperationType.UPDATE);
    }
    
    @Test
    public void testEarlierExistingWithSameValuesPlansNothing() {
        Planner p = new UpsertPlanner(new Properties());
        
        List<GenericRecord> existing = Lists.newArrayList();
        GenericRecord exist = createTestRecord();
        exist.put("key", "a");
        exist.put("timestamp", 50L);
        exist.put("value", "world");
        existing.add(exist);
        
        List<GenericRecord> arriving = Lists.newArrayList();
        GenericRecord arrived = createTestRecord();
        arrived.put("key", "a");
        arrived.put("timestamp", 100L);
        arrived.put("value", "world");
        arriving.add(arrived);
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        rm.setTimestampFieldName("timestamp");
        rm.setValueFieldNames(Lists.newArrayList("value"));
        rm.setLastUpdatedFieldName("lastupdated");
        
        List<PlannedRecord> planned = p.planOperations(arriving, existing, rm);
        
        assertEquals(planned.size(), 0);
    }
    
    @Test
    public void testLaterExistingPlansNothing() {
        Planner p = new UpsertPlanner(new Properties());
        
        List<GenericRecord> existing = Lists.newArrayList();
        GenericRecord exist = createTestRecord();
        exist.put("key", "a");
        exist.put("timestamp", 150L);
        exist.put("value", "world");
        existing.add(exist);
        
        List<GenericRecord> arriving = Lists.newArrayList();
        GenericRecord arrived = createTestRecord();
        arrived.put("key", "a");
        arrived.put("timestamp", 100L);
        arrived.put("value", "hello");
        arriving.add(arrived);
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        rm.setTimestampFieldName("timestamp");
        rm.setValueFieldNames(Lists.newArrayList("value"));
        rm.setLastUpdatedFieldName("lastupdated");
        
        List<PlannedRecord> planned = p.planOperations(arriving, existing, rm);
        
        assertEquals(planned.size(), 0);
    }
    
    @Test
    public void testSameTimeExistingWithNewValuesPlansUpdate() {
        Planner p = new UpsertPlanner(new Properties());
        
        List<GenericRecord> existing = Lists.newArrayList();
        GenericRecord exist = createTestRecord();
        exist.put("key", "a");
        exist.put("timestamp", 100L);
        exist.put("value", "world");
        existing.add(exist);
        
        List<GenericRecord> arriving = Lists.newArrayList();
        GenericRecord arrived = createTestRecord();
        arrived.put("key", "a");
        arrived.put("timestamp", 100L);
        arrived.put("value", "hello");
        arriving.add(arrived);
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        rm.setTimestampFieldName("timestamp");
        rm.setValueFieldNames(Lists.newArrayList("value"));
        rm.setLastUpdatedFieldName("lastupdated");
        
        List<PlannedRecord> planned = p.planOperations(arriving, existing, rm);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getOperationType(), OperationType.UPDATE);
    }
    
    @Test
    public void testSameTimeExistingWithSameValuesPlansNothing() {
        Planner p = new UpsertPlanner(new Properties());
        
        List<GenericRecord> existing = Lists.newArrayList();
        GenericRecord exist = createTestRecord();
        exist.put("key", "a");
        exist.put("timestamp", 100L);
        exist.put("value", "world");
        existing.add(exist);
        
        List<GenericRecord> arriving = Lists.newArrayList();
        GenericRecord arrived = createTestRecord();
        arrived.put("key", "a");
        arrived.put("timestamp", 100L);
        arrived.put("value", "world");
        arriving.add(arrived);
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        rm.setTimestampFieldName("timestamp");
        rm.setValueFieldNames(Lists.newArrayList("value"));
        rm.setLastUpdatedFieldName("lastupdated");
        
        List<PlannedRecord> planned = p.planOperations(arriving, existing, rm);
        
        assertEquals(planned.size(), 0);
    }
    
    @Test
    public void testOnlyUsesLatestArrivingRecordForAKey() {
        Planner p = new UpsertPlanner(new Properties());
        
        List<GenericRecord> existing = Lists.newArrayList();
        GenericRecord exist = createTestRecord();
        exist.put("key", "a");
        exist.put("timestamp", 50L);
        exist.put("value", "world");
        existing.add(exist);
        
        List<GenericRecord> arriving = Lists.newArrayList();
        for (Long timestamp : Lists.newArrayList(125L, 200L, 135L)) {
            GenericRecord arrived = createTestRecord();
            arrived.put("key", "a");
            arrived.put("timestamp", timestamp);
            arrived.put("value", "Value for timestamp " + timestamp);
            arriving.add(arrived);
        }
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        rm.setTimestampFieldName("timestamp");
        rm.setValueFieldNames(Lists.newArrayList("value"));
        rm.setLastUpdatedFieldName("lastupdated");
        
        List<PlannedRecord> planned = p.planOperations(arriving, existing, rm);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getOperationType(), OperationType.UPDATE);
        assertEquals(planned.get(0).get("value"), "Value for timestamp 200");
    }
    
    @Test
    public void testLastUpdated() {
        Planner p = new UpsertPlanner(new Properties());
        
        List<GenericRecord> existing = Lists.newArrayList();
                
        List<GenericRecord> arriving = Lists.newArrayList();
        GenericRecord record = createTestRecord();
        record.put("key", "a");
        record.put("timestamp", 100L);
        record.put("value", "hello");
        arriving.add(record);
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        rm.setTimestampFieldName("timestamp");
        rm.setValueFieldNames(Lists.newArrayList("value"));
        rm.setLastUpdatedFieldName("lastupdated");
        
        List<PlannedRecord> planned = p.planOperations(arriving, existing, rm);
        
        assertEquals(planned.size(), 1);
        assertNotNull(planned.get(0).get("lastupdated"));
    }
    
    @Test
    public void testNoLastUpdated() {
        Planner p = new UpsertPlanner(new Properties());
        
        List<GenericRecord> existing = Lists.newArrayList();
                
        List<GenericRecord> arriving = Lists.newArrayList();
        GenericRecord record = createTestRecord();
        record.put("key", "a");
        record.put("timestamp", 100L);
        record.put("value", "hello");
        arriving.add(record);
        
        RecordModel rm = new RecordModel();
        rm.setKeyFieldNames(Lists.newArrayList("key"));
        rm.setTimestampFieldName("timestamp");
        rm.setValueFieldNames(Lists.newArrayList("value"));
        
        List<PlannedRecord> planned = p.planOperations(arriving, existing, rm);
        
        assertEquals(planned.size(), 1);
        assertNull(planned.get(0).get("lastupdated"));
    }
    
    private GenericRecord createTestRecord() {
        Schema schema = SchemaBuilder.builder().record("test").fields()
                .optionalString("key")
                .optionalString("value")
                .optionalLong("timestamp")
                .optionalString("lastupdated")
                .endRecord();
        
        GenericRecord record = new GenericData.Record(schema);
        
        return record;
    }
    
}
