package com.cloudera.labs.envelope.plan.random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.labs.envelope.plan.EventTimeUpsertPlanner;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.cloudera.labs.envelope.plan.RandomPlanner;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestEventTimeUpsertPlanner {
    
    Row key;
    List<Row> arriving;
    List<Row> existing;
    StructType keySchema;
    StructType recordSchema;
    Map<String, Object> configMap;
    Config config;
    RandomPlanner p;
    
    @Before
    public void before() { 
        arriving = Lists.newArrayList();
        existing = Lists.newArrayList();
        
        keySchema = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("key", DataTypes.StringType, false)));
        recordSchema = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("key", DataTypes.StringType, false),
            DataTypes.createStructField("value", DataTypes.StringType, true),
            DataTypes.createStructField("timestamp", DataTypes.LongType, true)));
        
        configMap = Maps.newHashMap();
        configMap.put(EventTimeUpsertPlanner.KEY_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("key"));
        configMap.put(EventTimeUpsertPlanner.VALUE_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("value"));
        configMap.put(EventTimeUpsertPlanner.TIMESTAMP_FIELD_NAME_CONFIG_NAME, "timestamp");
        config = ConfigFactory.parseMap(configMap);
    }
    
    @Test
    public void testNotExisting() {
        p = new EventTimeUpsertPlanner();
        p.configure(config);

        arriving.add(new RowWithSchema(recordSchema, "a", "hello", 100L));
        Row key = new RowWithSchema(keySchema, "a");
        
        List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getMutationType(), MutationType.INSERT);
    }
    
    @Test
    public void testEarlierExistingWithNewValues() {
        p = new EventTimeUpsertPlanner();
        p.configure(config);

        existing.add(new RowWithSchema(recordSchema, "a", "world", 50L));
        arriving.add(new RowWithSchema(recordSchema, "a", "hello", 100L));
        Row key = new RowWithSchema(keySchema, "a");
        
        List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    }
    
    @Test
    public void testEarlierExistingWithSameValues() {
        p = new EventTimeUpsertPlanner();
        p.configure(config);

        existing.add(new RowWithSchema(recordSchema, "a", "world", 50L));
        arriving.add(new RowWithSchema(recordSchema, "a", "world", 100L));
        Row key = new RowWithSchema(keySchema, "a");
        
        List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);
        
        assertEquals(planned.size(), 0);
    }
    
    @Test
    public void testLaterExisting() {
        p = new EventTimeUpsertPlanner();
        p.configure(config);

        existing.add(new RowWithSchema(recordSchema, "a", "world", 150L));
        arriving.add(new RowWithSchema(recordSchema, "a", "hello", 100L));
        Row key = new RowWithSchema(keySchema, "a");
        
        List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);
        
        assertEquals(planned.size(), 0);
    }
    
    @Test
    public void testSameTimeExistingWithNewValues() {
        p = new EventTimeUpsertPlanner();
        p.configure(config);

        existing.add(new RowWithSchema(recordSchema, "a", "world", 100L));
        arriving.add(new RowWithSchema(recordSchema, "a", "hello", 100L));
        Row key = new RowWithSchema(keySchema, "a");
        
        List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
    }
    
    @Test
    public void testSameTimeExistingWithSameValues() {
        p = new EventTimeUpsertPlanner();
        p.configure(config);
        
        existing.add(new RowWithSchema(recordSchema, "a", "world", 100L));
        arriving.add(new RowWithSchema(recordSchema, "a", "world", 100L));
        Row key = new RowWithSchema(keySchema, "a");
        
        List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);
        
        assertEquals(planned.size(), 0);
    }
    
    @Test
    public void testOnlyUsesLatestArrivingRecordForAKey() {
        p = new EventTimeUpsertPlanner();
        p.configure(config);

        existing.add(new RowWithSchema(recordSchema, "a", "world", 50L));
        arriving.add(new RowWithSchema(recordSchema, "a", "125", 125L));
        arriving.add(new RowWithSchema(recordSchema, "a", "200", 200L));
        arriving.add(new RowWithSchema(recordSchema, "a", "135", 135L));
        Row key = new RowWithSchema(keySchema, "a");
        
        List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getMutationType(), MutationType.UPDATE);
        Row plannedRow = planned.get(0).getRow();
        assertEquals(plannedRow.get(plannedRow.fieldIndex("value")), "200");
    }
    
    @Test
    public void testLastUpdated() {
        configMap.put(EventTimeUpsertPlanner.LAST_UPDATED_FIELD_NAME_CONFIG_NAME, "lastupdated");
        config = ConfigFactory.parseMap(configMap);
        p = new EventTimeUpsertPlanner();
        p.configure(config);

        arriving.add(new RowWithSchema(recordSchema, "a", "hello", 100L));
        Row key = new RowWithSchema(keySchema, "a");
        
        List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);
        
        assertEquals(planned.size(), 1);
        Row plannedRow = planned.get(0).getRow();
        assertNotNull(plannedRow.get(plannedRow.fieldIndex("lastupdated")));
    }
    
    @Test
    public void testNoLastUpdated() {
        p = new EventTimeUpsertPlanner();
        p.configure(config);

        arriving.add(new RowWithSchema(recordSchema, "a", "hello", 100L));
        Row key = new RowWithSchema(keySchema, "a");
        
        List<PlannedRow> planned = p.planMutationsForKey(key, arriving, existing);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getRow().length(), 3);
    }
    
}
