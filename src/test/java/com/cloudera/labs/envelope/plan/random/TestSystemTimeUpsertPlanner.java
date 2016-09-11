package com.cloudera.labs.envelope.plan.random;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.cloudera.labs.envelope.plan.random.RandomWritePlanner;
import com.cloudera.labs.envelope.plan.random.SystemTimeUpsertPlanner;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestSystemTimeUpsertPlanner {

    @Test
    public void testPlansUpserts() {
        Config config = ConfigFactory.empty();
        RandomWritePlanner planner = new SystemTimeUpsertPlanner(config);
        
        StructType schema = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("key", DataTypes.StringType, false)));
        Row row = new RowWithSchema(schema, "a");
        
        List<PlannedRow> planned = planner.planMutationsForRow(row);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getMutationType(), MutationType.UPSERT);
    }
    
    @Test
    public void testNoLastUpdated() {
        Config config = ConfigFactory.empty();
        RandomWritePlanner planner = new SystemTimeUpsertPlanner(config);
        
        StructType schema = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("key", DataTypes.StringType, false)));
        Row row = new RowWithSchema(schema, "a");
        
        List<PlannedRow> planned = planner.planMutationsForRow(row);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getRow().length(), 1);
    }
    
    @Test
    public void testLastUpdated() {
        Map<String, Object> configMap = Maps.newHashMap();
        configMap.put(SystemTimeUpsertPlanner.LAST_UPDATED_FIELD_NAME_CONFIG_NAME, "lastupdated");
        Config config = ConfigFactory.parseMap(configMap);
        
        RandomWritePlanner planner = new SystemTimeUpsertPlanner(config);
        
        StructType schema = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("key", DataTypes.StringType, false)));
        Row row = new RowWithSchema(schema, "a");
        
        List<PlannedRow> planned = planner.planMutationsForRow(row);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0).getRow().length(), 2);
    }
    
}
