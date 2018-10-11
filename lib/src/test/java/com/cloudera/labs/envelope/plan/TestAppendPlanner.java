/*
 * Copyright (c) 2015-2018, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.labs.envelope.plan;

import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestAppendPlanner {

  private static Dataset<Row> dataFrame;

  @BeforeClass
  public static void beforeClass() {
    StructType schema = RowUtils.structTypeFor(Lists.newArrayList("key", "value"), Lists.newArrayList("string", "int"));
    Row row = new RowWithSchema(schema, null, 1);
    dataFrame = Contexts.getSparkSession().createDataFrame(Lists.newArrayList(row), schema);
  }

  @Test
  public void testPlansInserts() {
    Config config = ConfigFactory.empty();
    AppendPlanner ap = new AppendPlanner();
    assertNoValidationFailures(ap, config);
    ap.configure(config);

    List<Tuple2<MutationType, Dataset<Row>>> planned = ap.planMutationsForSet(dataFrame);

    assertEquals(planned.size(), 1);
    assertEquals(planned.get(0)._1(), MutationType.INSERT);
    assertEquals(planned.get(0)._2().count(), 1);
  }

  @Test
  public void testUUIDKey() {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(AppendPlanner.UUID_KEY_CONFIG_NAME, true);
    configMap.put(AppendPlanner.KEY_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("key"));
    Config config = ConfigFactory.parseMap(configMap);

    AppendPlanner ap = new AppendPlanner();
    assertNoValidationFailures(ap, config);
    ap.configure(config);

    List<Tuple2<MutationType, Dataset<Row>>> planned = ap.planMutationsForSet(dataFrame);

    assertEquals(planned.size(), 1);

    Dataset<Row> plannedDF = planned.get(0)._2();

    assertEquals(planned.get(0)._1(), MutationType.INSERT);
    assertEquals(plannedDF.count(), 1);

    Row plannedRow = plannedDF.collectAsList().get(0);

    assertNotNull(plannedRow.get(plannedRow.fieldIndex("key")));
    assertEquals(plannedRow.getString(plannedRow.fieldIndex("key")).length(), 36);
  }

  @Test
  public void testNoUUIDKey() {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(AppendPlanner.KEY_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("key"));
    Config config = ConfigFactory.parseMap(configMap);

    AppendPlanner ap = new AppendPlanner();
    assertNoValidationFailures(ap, config);
    ap.configure(config);

    List<Tuple2<MutationType, Dataset<Row>>> planned = ap.planMutationsForSet(dataFrame);

    assertEquals(planned.size(), 1);

    Dataset<Row> plannedDF = planned.get(0)._2();

    assertEquals(planned.get(0)._1(), MutationType.INSERT);
    assertEquals(plannedDF.count(), 1);

    Row plannedRow = plannedDF.collectAsList().get(0);

    assertNull(plannedRow.get(plannedRow.fieldIndex("key")));
  }

  @Test
  public void testLastUpdated() {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(AppendPlanner.LAST_UPDATED_FIELD_NAME_CONFIG_NAME, "lastupdated");
    Config config = ConfigFactory.parseMap(configMap);

    AppendPlanner ap = new AppendPlanner();
    assertNoValidationFailures(ap, config);
    ap.configure(config);

    List<Tuple2<MutationType, Dataset<Row>>> planned = ap.planMutationsForSet(dataFrame);

    assertEquals(planned.size(), 1);

    Dataset<Row> plannedDF = planned.get(0)._2();

    assertEquals(planned.get(0)._1(), MutationType.INSERT);
    assertEquals(plannedDF.count(), 1);

    Row plannedRow = plannedDF.collectAsList().get(0);

    assertNotNull(plannedRow.get(plannedRow.fieldIndex("lastupdated")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNoLastUpdated() {
    Config config = ConfigFactory.empty();
    AppendPlanner ap = new AppendPlanner();
    assertNoValidationFailures(ap, config);
    ap.configure(config);

    List<Tuple2<MutationType, Dataset<Row>>> planned = ap.planMutationsForSet(dataFrame);

    assertEquals(planned.size(), 1);

    Dataset<Row> plannedDF = planned.get(0)._2();

    assertEquals(planned.get(0)._1(), MutationType.INSERT);
    assertEquals(plannedDF.count(), 1);

    Row plannedRow = plannedDF.collectAsList().get(0);
    plannedRow.get(plannedRow.fieldIndex("lastupdated"));
  }

}
