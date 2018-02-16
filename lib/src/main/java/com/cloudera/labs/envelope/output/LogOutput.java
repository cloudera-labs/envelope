/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.output;

import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.plan.MutationType;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

public class LogOutput implements BulkOutput, ProvidesAlias {

  public static final String DELIMITER_CONFIG_NAME = "delimiter";
  public static final String LOG_LEVEL_CONFIG_NAME = "level";

  private Config config;

  private static Logger LOG = LoggerFactory.getLogger(LogOutput.class);


  @Override
  public void configure(Config config) {
    this.config = config;
  }

  @Override
  public void applyBulkMutations(List<Tuple2<MutationType, Dataset<Row>>> planned) {
    for (Tuple2<MutationType, Dataset<Row>> mutation : planned) {
      MutationType mutationType = mutation._1();
      Dataset<Row> mutationDF = mutation._2();

      if (mutationType.equals(MutationType.INSERT)) {
        mutationDF.javaRDD().foreach(new SendRowToLogFunction(getDelimiter(), getLogLevel()));
      }
    }
  }

  @Override
  public Set<MutationType> getSupportedBulkMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT);
  }

  private String getDelimiter() {
    if (!config.hasPath(DELIMITER_CONFIG_NAME)) return ",";

    return config.getString(DELIMITER_CONFIG_NAME);
  }

  private String getLogLevel() {
    if (!config.hasPath(LOG_LEVEL_CONFIG_NAME)) return "INFO";

    return config.getString(LOG_LEVEL_CONFIG_NAME).toUpperCase();
  }

  @Override
  public String getAlias() {
    return "log";
  }

  @SuppressWarnings("serial")
  private static class SendRowToLogFunction implements VoidFunction<Row> {
    private Joiner joiner;
    private String delimiter;
    private String logLevel;


    public SendRowToLogFunction(String delimiter, String logLevel) {
      this.delimiter = delimiter;
      this.logLevel = logLevel;
    }

    @Override
    public void call(Row mutation) throws Exception {
      if (joiner == null) {
        joiner = Joiner.on(delimiter);
      }

      List<Object> values = Lists.newArrayList();

      for (int fieldIndex = 0; fieldIndex < mutation.size(); fieldIndex++) {
        values.add(mutation.get(fieldIndex));
      }
      String log = joiner.join(values);

      switch (logLevel) {
        case "TRACE":
          LOG.trace(log);
          break;
        case "DEBUG":
          LOG.debug(log);
          break;
        case "INFO":
          LOG.info(log);
          break;
        case "WARN":
          LOG.warn(log);
          break;
        case "ERROR":
          LOG.error(log);
          break;
        default:
          throw new RuntimeException("Invalid log level: " + logLevel);
      }
    }
  }

}
