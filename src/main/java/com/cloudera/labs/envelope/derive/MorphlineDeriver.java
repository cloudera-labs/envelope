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
package com.cloudera.labs.envelope.derive;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.MorphlineUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.typesafe.config.Config;

/**
 *
 */
public class MorphlineDeriver implements Deriver {

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineDeriver.class);

  public static final String MORPHLINE = "morphline.file";
  public static final String MORPHLINE_ID = "morphline.id";
  public static final String PRODUCTION_MODE = "production.mode";
  public static final String FIELD_NAMES = "field.names";
  public static final String FIELD_TYPES = "field.types";

  private StructType schema;
  private String morphlineFile;
  private String morphlineId;

  @Override
  public void configure(Config config) {
    LOG.trace("Configuring Morphline Deriver");

    // Set up the Morphline configuration, the file must be located on the local file system
    this.morphlineFile = config.getString(MORPHLINE);
    this.morphlineId = config.getString(MORPHLINE_ID);

    if (this.morphlineFile == null || this.morphlineFile.trim().length() == 0) {
      throw new MorphlineCompilationException("Missing or empty Morphline File configuration parameter", null);
    }

    // Construct the StructType schema for the Rows
    List<String> fieldNames = config.getStringList(FIELD_NAMES);
    List<String> fieldTypes = config.getStringList(FIELD_TYPES);
    this.schema = RowUtils.structTypeFor(fieldNames, fieldTypes);
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    LOG.debug("Executing on Dependencies {}", dependencies.keySet());

    // Get the DF
    if (dependencies.size() != 1) {
      throw new RuntimeException("MorphlineDeriver must have only one dependency");
    }
    Dataset<Row> inputDF = dependencies.values().iterator().next();

    // For each partition in the DataFrame / RDD
    JavaRDD<Row> outputRDD = inputDF.toJavaRDD().flatMap(
        MorphlineUtils.morphlineMapper(this.morphlineFile, this.morphlineId, getSchema()));

    // Convert all the Rows into a new DataFrame
    return Contexts.getSparkSession().createDataFrame(outputRDD, getSchema());
  }

  /**
   *
   * @return The generated StructType for the resulting DataFrame
   */
  protected StructType getSchema() {
    return this.schema;
  }

}
