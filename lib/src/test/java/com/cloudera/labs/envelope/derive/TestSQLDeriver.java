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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.Test;

import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;

public class TestSQLDeriver {

  @Test
  public void testQueryLiteral() throws Exception {
    Contexts.getSparkSession().createDataset(Lists.newArrayList(1), Encoders.INT()).createOrReplaceTempView("literaltable");

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(SQLDeriver.QUERY_LITERAL_CONFIG_NAME, "SELECT * FROM literaltable");
    Config config = ConfigFactory.parseMap(configMap);

    SQLDeriver deriver = new SQLDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    Object result = deriver.derive(Maps.<String, Dataset<Row>>newHashMap()).collectAsList().get(0).get(0);

    assertEquals(1, result);
  }

  @Test
  public void testQueryFile() throws Exception {
    Contexts.getSparkSession().createDataset(Lists.newArrayList(1), Encoders.INT()).createOrReplaceTempView("literaltable");

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(SQLDeriver.QUERY_FILE_CONFIG_NAME, getClass().getResource("/sql/query_without_parameters.sql").getPath());
    Config config = ConfigFactory.parseMap(configMap);

    SQLDeriver deriver = new SQLDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    Object result = deriver.derive(Maps.<String, Dataset<Row>>newHashMap()).collectAsList().get(0).get(0);

    assertEquals(1, result);
  }

  @Test
  public void testParameters() throws Exception {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(SQLDeriver.QUERY_FILE_CONFIG_NAME, getClass().getResource("/sql/query_with_parameters.sql").getPath());
    configMap.put(SQLDeriver.PARAMETER_PREFIX_CONFIG_NAME + ".param1", "val1");
    configMap.put(SQLDeriver.PARAMETER_PREFIX_CONFIG_NAME + ".param2", "val2");
    Config config = ConfigFactory.parseMap(configMap);

    SQLDeriver deriver = new SQLDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    Row result = deriver.derive(Maps.<String, Dataset<Row>>newHashMap()).collectAsList().get(0);

    assertEquals(RowFactory.create("val1", "val2", "val1"), result);
  }

}
