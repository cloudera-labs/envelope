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

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.MorphlineUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MorphlineDeriver implements Deriver, ProvidesAlias, ProvidesValidations {

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineDeriver.class);

  public static final String STEP_NAME_CONFIG = "step.name";
  public static final String MORPHLINE = "morphline.file";
  public static final String MORPHLINE_ID = "morphline.id";
  public static final String FIELD_NAMES = "field.names";
  public static final String FIELD_TYPES = "field.types";
  public static final String ERROR_ON_EMPTY = "error.on.empty";

  private String stepName;
  private StructType schema;
  private String morphlineFile;
  private String morphlineId;
  private boolean errorOnEmpty;

  @Override
  public void configure(Config config) {
    LOG.trace("Configuring Morphline Deriver");

    // Designate which dependency step to act upon
    this.stepName = config.getString(STEP_NAME_CONFIG);

    // Set up the Morphline configuration, the file must be located on the local file system
    this.morphlineFile = config.getString(MORPHLINE);
    this.morphlineId = config.getString(MORPHLINE_ID);

    // Construct the StructType schema for the Rows
    List<String> fieldNames = config.getStringList(FIELD_NAMES);
    List<String> fieldTypes = config.getStringList(FIELD_TYPES);
    this.schema = RowUtils.structTypeFor(fieldNames, fieldTypes);

    errorOnEmpty = ConfigUtils.getOrElse(config, ERROR_ON_EMPTY, true);
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    if (!dependencies.containsKey(stepName)) {
      throw new RuntimeException("Step not found in the dependencies list");
    }

    Dataset<Row> sourceStep = dependencies.get(stepName);

    // For each partition in the DataFrame / RDD
    JavaRDD<Row> outputRDD = sourceStep.toJavaRDD().flatMap(
        MorphlineUtils.morphlineMapper(this.morphlineFile, this.morphlineId, getSchema(), errorOnEmpty));

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

  @Override
  public String getAlias() {
    return "morphline";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(STEP_NAME_CONFIG, ConfigValueType.STRING)
        .mandatoryPath(MORPHLINE, ConfigValueType.STRING)
        .mandatoryPath(MORPHLINE_ID, ConfigValueType.STRING)
        .mandatoryPath(FIELD_NAMES, ConfigValueType.LIST)
        .mandatoryPath(FIELD_TYPES, ConfigValueType.LIST)
        .optionalPath(ERROR_ON_EMPTY, ConfigValueType.BOOLEAN)
        .build();
  }
  
}
