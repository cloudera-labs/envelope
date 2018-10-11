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

package com.cloudera.labs.envelope.run;

import com.cloudera.labs.envelope.derive.Deriver;
import com.cloudera.labs.envelope.derive.DeriverFactory;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestRunner {

  @Before
  public void before() {
    Contexts.closeSparkSession(true);
  }

  @Test
  public void testValidUDFs() throws Exception {    
    Config config = ConfigUtils.configFromResource("/udf/udf_valid.conf");

    Runner.initializeUDFs(config);
    Deriver deriver = DeriverFactory.create(config.getConfig(Runner.STEPS_SECTION_CONFIG + ".runudf.deriver"), true);
    Dataset<Row> derived = deriver.derive(Maps.<String, Dataset<Row>>newHashMap());

    assertEquals(RowFactory.create("hello", 1), derived.collectAsList().get(0));
  }
  
  @Test
  (expected = AnalysisException.class)
  public void testNoUDFs() throws Throwable {
    Config config = ConfigUtils.configFromResource("/udf/udf_none.conf");

    Runner.initializeUDFs(config);
    Deriver deriver = DeriverFactory.create(config.getConfig("steps.runudf.deriver"), true);
    deriver.derive(Maps.<String, Dataset<Row>>newHashMap());
  }
  
  @SuppressWarnings("serial")
  public static class TestUDF1 implements UDF1<String, String> {
    @Override
    public String call(String arg) throws Exception {
      return arg;
    }
  }
  
  @SuppressWarnings("serial")
  public static class TestUDF2 implements UDF1<Integer, Integer> {
    @Override
    public Integer call(Integer arg) throws Exception {
      return arg;
    }
  }
  
  public static class SQLDeriver implements Deriver {
    private Config config;

    @Override
    public void configure(Config config) {
      this.config = config;
    }

    @Override
    public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
      String query = config.getString("query.literal");
      Dataset<Row> derived = Contexts.getSparkSession().sql(query);
      return derived;
    }
  }
  
}
