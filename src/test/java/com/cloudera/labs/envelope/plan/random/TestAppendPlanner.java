package com.cloudera.labs.envelope.plan.random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.cloudera.labs.envelope.plan.random.AppendPlanner;
import com.cloudera.labs.envelope.plan.random.RandomWritePlanner;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestAppendPlanner {

    @Test
    public void testPlansInserts() {
        Config config = ConfigFactory.empty();
        RandomWritePlanner ap = new AppendPlanner(config);
        
        Row row = createTestRow();
        
        List<PlannedRow> planned = ap.planMutationsForRow(row);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getMutationType(), MutationType.INSERT);
    }
    
    @Test
    public void testUUIDKey() {
        Map<String, Object> configMap = Maps.newHashMap();
        configMap.put(AppendPlanner.UUID_KEY_CONFIG_NAME, "true");
        configMap.put(AppendPlanner.KEY_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("key"));
        Config config = ConfigFactory.parseMap(configMap);
        
        RandomWritePlanner ap = new AppendPlanner(config);
        
        Row row = createTestRow();
        
        List<PlannedRow> planned = ap.planMutationsForRow(row);
        
        assertEquals(planned.size(), 1);
        Row planRow = planned.get(0).getRow();
        assertNotNull(planRow.get(planRow.fieldIndex("key")));
        assertEquals(planRow.getString(planRow.fieldIndex("key")).length(), 36);
    }
    
    @Test
    public void testNoUUIDKey() {
        Map<String, Object> configMap = Maps.newHashMap();
        configMap.put(AppendPlanner.KEY_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("key"));
        Config config = ConfigFactory.parseMap(configMap);
        
        RandomWritePlanner ap = new AppendPlanner(config);
        
        Row row = createTestRow();
        
        List<PlannedRow> planned = ap.planMutationsForRow(row);
        
        assertEquals(planned.size(), 1);
        Row planRow = planned.get(0).getRow();
        assertNull(planRow.get(planRow.fieldIndex("key")));
    }
    
    @Test
    public void testLastUpdated() {
        Map<String, Object> configMap = Maps.newHashMap();
        configMap.put(AppendPlanner.LAST_UPDATED_FIELD_NAME_CONFIG_NAME, "lastupdated");
        Config config = ConfigFactory.parseMap(configMap);
        
        RandomWritePlanner ap = new AppendPlanner(config);
        
        Row row = createTestRow();
        
        List<PlannedRow> planned = ap.planMutationsForRow(row);
        
        assertEquals(planned.size(), 1);
        Row planRow = planned.get(0).getRow();
        assertNotNull(planRow.get(planRow.fieldIndex("lastupdated")));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testNoLastUpdated() {
        Config config = ConfigFactory.empty();
        RandomWritePlanner ap = new AppendPlanner(config);
        
        Row row = createTestRow();
        
        List<PlannedRow> planned = ap.planMutationsForRow(row);
        
        assertEquals(planned.size(), 1);
        Row planRow = planned.get(0).getRow();
        planRow.get(planRow.fieldIndex("lastupdated"));
    }
    
    private Row createTestRow() {
        StructType schema = RowUtils.structTypeFor(Lists.newArrayList("key", "value"), Lists.newArrayList("string", "int"));
        Row row = new RowWithSchema(schema, null, 1);
        
        return row;
    }
    
}
