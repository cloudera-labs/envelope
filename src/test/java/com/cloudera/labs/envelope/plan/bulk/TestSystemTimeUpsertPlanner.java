package com.cloudera.labs.envelope.plan.bulk;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.labs.envelope.plan.BulkPlanner;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.SystemTimeUpsertPlanner;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import scala.Tuple2;

public class TestSystemTimeUpsertPlanner {

  private static JavaSparkContext jsc;
  private static DataFrame dataFrame;

  @BeforeClass
  public static void beforeClass() {
    SparkConf conf = new SparkConf();
    conf.setMaster("local[1]");
    conf.setAppName("TestSystemTimeUpsertPlanner");
    jsc = new JavaSparkContext(conf);
    SQLContext sqlc = new SQLContext(jsc);

    StructType schema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", DataTypes.StringType, false)));
    Row row = new RowWithSchema(schema, "a");
    dataFrame = sqlc.createDataFrame(Lists.newArrayList(row), schema);
  }

  @AfterClass
  public static void afterClass() {
    jsc.close();
  }

  @Test
  public void testPlansUpserts() {
    Config config = ConfigFactory.empty();
    BulkPlanner planner = new SystemTimeUpsertPlanner();
    planner.configure(config);

    List<Tuple2<MutationType, DataFrame>> planned = planner.planMutationsForSet(dataFrame);

    assertEquals(planned.size(), 1);
    assertEquals(planned.get(0)._1(), MutationType.UPSERT);
    assertEquals(planned.get(0)._2().count(), 1);
  }

  @Test
  public void testNoLastUpdated() {
    Config config = ConfigFactory.empty();
    BulkPlanner planner = new SystemTimeUpsertPlanner();
    planner.configure(config);

    List<Tuple2<MutationType, DataFrame>> planned = planner.planMutationsForSet(dataFrame);

    assertEquals(planned.size(), 1);

    DataFrame plannedDF = planned.get(0)._2();

    assertEquals(plannedDF.count(), 1);

    Row plannedRow = plannedDF.collect()[0];
    assertEquals(plannedRow.length(), 1);
  }

  @Test
  public void testLastUpdated() {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(SystemTimeUpsertPlanner.LAST_UPDATED_FIELD_NAME_CONFIG_NAME, "lastupdated");
    Config config = ConfigFactory.parseMap(configMap);

    BulkPlanner planner = new SystemTimeUpsertPlanner();
    planner.configure(config);

    List<Tuple2<MutationType, DataFrame>> planned = planner.planMutationsForSet(dataFrame);

    assertEquals(planned.size(), 1);

    DataFrame plannedDF = planned.get(0)._2();

    assertEquals(plannedDF.count(), 1);

    Row plannedRow = plannedDF.collect()[0];
    assertEquals(plannedRow.length(), 2);
  }

}
