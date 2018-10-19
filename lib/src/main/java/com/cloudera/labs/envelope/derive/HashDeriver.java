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
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Map;

public class HashDeriver implements Deriver, ProvidesAlias, ProvidesValidations {

  public static final String STEP_NAME_CONFIG = "step";
  public static final String HASH_FIELD_NAME_CONFIG = "hash-field";
  public static final String DELIMITER_CONFIG = "delimiter";
  public static final String NULL_STRING_CONFIG = "null-string";

  public static final String DEFAULT_HASH_FIELD_NAME = "hash";
  public static final String DEFAULT_DELIMITER = "";
  public static final String DEFAULT_NULL_STRING = "__NULL__";

  public static final String HASH_DERIVER_ALIAS = "hash";

  private String stepName;
  private String hashFieldName;
  private String delimiter;
  private String nullString;

  @Override
  public void configure(Config config) {
    this.stepName = ConfigUtils.getOrNull(config, STEP_NAME_CONFIG);
    this.hashFieldName = ConfigUtils.getOrElse(config, HASH_FIELD_NAME_CONFIG, DEFAULT_HASH_FIELD_NAME);
    this.delimiter = ConfigUtils.getOrElse(config, DELIMITER_CONFIG, DEFAULT_DELIMITER);
    this.nullString = ConfigUtils.getOrElse(config, NULL_STRING_CONFIG, DEFAULT_NULL_STRING);
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) {
    String concatenatedFieldName = "_concatenated";

    Dataset<Row> dependency = getStepDataFrame(dependencies);

    Dataset<Row> concatenated = dependency.map(new ConcatenationFunction(delimiter, nullString),
        RowEncoder.apply(dependency.schema().add(concatenatedFieldName, DataTypes.BinaryType)));

    return concatenated
        .withColumn(hashFieldName, functions.md5(functions.col(concatenatedFieldName)))
        .drop(concatenatedFieldName);
  }

  private Dataset<Row> getStepDataFrame(Map<String, Dataset<Row>> dependencies) {
    if (stepName != null) {
      if (!dependencies.containsKey(stepName)) {
        throw new RuntimeException("Hash deriver does not have step '" + stepName +
            "' in its dependencies");
      }
      return dependencies.get(stepName);
    }
    else {
      if (dependencies.size() != 1) {
        throw new RuntimeException(
            "Hash deriver must specify a step if it does not only have one dependency");
      }
      return dependencies.values().iterator().next();
    }
  }

  private static class ConcatenationFunction implements MapFunction<Row, Row> {
    private StringBuilder sb = new StringBuilder();
    private String delimiter;
    private String nullString;

    public ConcatenationFunction(String delimiter, String nullString) {
      this.delimiter = delimiter;
      this.nullString = nullString;
    }

    @Override
    public Row call(Row toHash) {
      sb.setLength(0);

      for (int fieldNum = 0; fieldNum < toHash.schema().size(); fieldNum++) {
        Object value = toHash.get(fieldNum);
        sb.append(value != null ? value : nullString);
        sb.append(delimiter);
      }

      return RowUtils.append(toHash, sb.toString().getBytes());
    }
  }

  @Override
  public String getAlias() {
    return HASH_DERIVER_ALIAS;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(STEP_NAME_CONFIG, ConfigValueType.STRING)
        .optionalPath(HASH_FIELD_NAME_CONFIG, ConfigValueType.STRING)
        .optionalPath(DELIMITER_CONFIG, ConfigValueType.STRING)
        .allowEmptyValue(DELIMITER_CONFIG)
        .optionalPath(NULL_STRING_CONFIG, ConfigValueType.STRING)
        .allowEmptyValue(NULL_STRING_CONFIG)
        .build();
  }

}
