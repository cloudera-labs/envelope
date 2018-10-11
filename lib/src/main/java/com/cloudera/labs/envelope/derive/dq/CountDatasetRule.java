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

package com.cloudera.labs.envelope.derive.dq;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;

import java.util.Map;

public class CountDatasetRule implements DatasetRule, ProvidesAlias, ProvidesValidations {

  private static final String EXPECTED_LITERAL_CONFIG = "expected.literal";
  private static final String EXPECTED_DEPENDENCY_CONFIG = "expected.dependency";

  private long expected = -1;
  private String dependency;
  private String name;

  @Override
  public void configure(String name, Config config) {
    this.name = name;
    if (config.hasPath(EXPECTED_LITERAL_CONFIG)) {
      expected = config.getLong(EXPECTED_LITERAL_CONFIG);
    }
    if (config.hasPath(EXPECTED_DEPENDENCY_CONFIG)) {
      dependency = config.getString(EXPECTED_DEPENDENCY_CONFIG);
    }
  }

  @Override
  public Dataset<Row> check(Dataset<Row> dataset, Map<String, Dataset<Row>> stepDependencies) {
    if (isDependency()) {
      Dataset<Row> expectedDependency = stepDependencies.get(dependency);
      if (expectedDependency.count() == 1 && expectedDependency.schema().fields().length == 1
          && expectedDependency.schema().apply(0).dataType() == DataTypes.LongType) {
        expected = expectedDependency.collectAsList().get(0).getLong(0);
      } else {
        throw new RuntimeException("Step dependency for count rule must have one row with a single field of long type");
      }
    }
    if (expected < 0) {
      throw new RuntimeException("Failed to determine expected count: must be specified either as literal or step dependency");
    }
    return dataset.groupBy().count().map(new CheckCount(expected, name), RowEncoder.apply(SCHEMA));
  }

  private boolean isDependency() {
    return dependency != null && !dependency.isEmpty();
  }

  @Override
  public String getAlias() {
    return "count";
  }

  private class CheckCount implements MapFunction<Row, Row> {

    private long thisExpected;
    private String name;

    CheckCount(long thisExpected, String name) {
      this.thisExpected = thisExpected;
      this.name = name;
    }

    @Override
    public Row call(Row row) throws Exception {
      return new RowWithSchema(SCHEMA, name, row.<Long>getAs("count") == thisExpected);
    }

  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(EXPECTED_LITERAL_CONFIG, ConfigValueType.NUMBER)
        .optionalPath(EXPECTED_DEPENDENCY_CONFIG, ConfigValueType.STRING)
        .exactlyOnePathExists(EXPECTED_LITERAL_CONFIG, EXPECTED_DEPENDENCY_CONFIG)
        .build();
  }

}
