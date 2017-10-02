/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.run;

import static org.junit.Assert.fail;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.junit.Test;

import com.cloudera.labs.envelope.derive.Deriver;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;

public class TestRunner {

  @Test
  public void testValidUDFs() throws Exception {    
    Config config = ConfigUtils.configFromResource("/udf/udf_valid.conf");
    
    Contexts.closeSparkSession(true);
    try {
      Runner.run(config);
    }
    catch (Exception e) {
      fail(e.getMessage());
    }
  }
  
  @Test
  (expected = AnalysisException.class)
  public void testNoUDFs() throws Throwable {
    Config config = ConfigUtils.configFromResource("/udf/udf_none.conf");

    Contexts.closeSparkSession(true);
    
    try {
      Runner.run(config);
    }
    // Data steps run off the main thread so we have to dig into the concurrency-related exception first
    catch (ExecutionException e) {
      throw e.getCause();
    }
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

    @Override
    public String getAlias() {
      return "sql";
    }
  }
  
}
