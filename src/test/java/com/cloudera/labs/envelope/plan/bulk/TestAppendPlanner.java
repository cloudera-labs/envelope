package com.cloudera.labs.envelope.plan.bulk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.labs.envelope.plan.AppendPlanner;
import com.cloudera.labs.envelope.plan.BulkPlanner;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import scala.Tuple2;

public class TestAppendPlanner {
    
    private static JavaSparkContext jsc;
    private static DataFrame dataFrame;
    
    @BeforeClass
    public static void beforeClass() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[1]");
        conf.setAppName("TestAppendPlanner");
        jsc = new JavaSparkContext(conf);
        SQLContext sqlc = new SQLContext(jsc);
        
        StructType schema = RowUtils.structTypeFor(Lists.newArrayList("key", "value"), Lists.newArrayList("string", "int"));
        Row row = new RowWithSchema(schema, null, 1);
        dataFrame = sqlc.createDataFrame(Lists.newArrayList(row), schema);
    }
    
    @AfterClass
    public static void afterClass() {
        jsc.close();
    }
    
    @Test
    public void testPlansInserts() {
        Config config = ConfigFactory.empty();
        BulkPlanner ap = new AppendPlanner();
        ap.configure(config);
        
        List<Tuple2<MutationType, DataFrame>> planned = ap.planMutationsForSet(dataFrame);
        
        assertEquals(planned.size(), 1);
        assertEquals(planned.get(0)._1(), MutationType.INSERT);
        assertEquals(planned.get(0)._2().count(), 1);
    }
    
    @Test
    public void testUUIDKey() {
        Map<String, Object> configMap = Maps.newHashMap();
        configMap.put(AppendPlanner.UUID_KEY_CONFIG_NAME, "true");
        configMap.put(AppendPlanner.KEY_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("key"));
        Config config = ConfigFactory.parseMap(configMap);
        
        BulkPlanner ap = new AppendPlanner();
        ap.configure(config);
        
        List<Tuple2<MutationType, DataFrame>> planned = ap.planMutationsForSet(dataFrame);
        
        assertEquals(planned.size(), 1);

        DataFrame plannedDF = planned.get(0)._2();
        
        assertEquals(planned.get(0)._1(), MutationType.INSERT);
        assertEquals(plannedDF.count(), 1);
        
        Row plannedRow = plannedDF.collect()[0];
        
        assertNotNull(plannedRow.get(plannedRow.fieldIndex("key")));
        assertEquals(plannedRow.getString(plannedRow.fieldIndex("key")).length(), 36);
    }
    
    @Test
    public void testNoUUIDKey() {
        Map<String, Object> configMap = Maps.newHashMap();
        configMap.put(AppendPlanner.KEY_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("key"));
        Config config = ConfigFactory.parseMap(configMap);
        
        BulkPlanner ap = new AppendPlanner();
        ap.configure(config);
        
        List<Tuple2<MutationType, DataFrame>> planned = ap.planMutationsForSet(dataFrame);
        
        assertEquals(planned.size(), 1);

        DataFrame plannedDF = planned.get(0)._2();
        
        assertEquals(planned.get(0)._1(), MutationType.INSERT);
        assertEquals(plannedDF.count(), 1);
        
        Row plannedRow = plannedDF.collect()[0];
        
        assertNull(plannedRow.get(plannedRow.fieldIndex("key")));
    }
    
    @Test
    public void testLastUpdated() {
        Map<String, Object> configMap = Maps.newHashMap();
        configMap.put(AppendPlanner.LAST_UPDATED_FIELD_NAME_CONFIG_NAME, "lastupdated");
        Config config = ConfigFactory.parseMap(configMap);
        
        BulkPlanner ap = new AppendPlanner();
        ap.configure(config);
        
        List<Tuple2<MutationType, DataFrame>> planned = ap.planMutationsForSet(dataFrame);
        
        assertEquals(planned.size(), 1);

        DataFrame plannedDF = planned.get(0)._2();
        
        assertEquals(planned.get(0)._1(), MutationType.INSERT);
        assertEquals(plannedDF.count(), 1);
        
        Row plannedRow = plannedDF.collect()[0];
        
        assertNotNull(plannedRow.get(plannedRow.fieldIndex("lastupdated")));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testNoLastUpdated() {
        Config config = ConfigFactory.empty();
        BulkPlanner ap = new AppendPlanner();
        ap.configure(config);
        
        List<Tuple2<MutationType, DataFrame>> planned = ap.planMutationsForSet(dataFrame);
        
        assertEquals(planned.size(), 1);

        DataFrame plannedDF = planned.get(0)._2();
        
        assertEquals(planned.get(0)._1(), MutationType.INSERT);
        assertEquals(plannedDF.count(), 1);
        
        Row plannedRow = plannedDF.collect()[0];
        plannedRow.get(plannedRow.fieldIndex("lastupdated"));
    }
    
}
