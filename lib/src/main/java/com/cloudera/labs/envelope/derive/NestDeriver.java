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
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NestDeriver implements Deriver, ProvidesAlias, ProvidesValidations {

  public static final String NEST_INTO_CONFIG_NAME = "nest.into";
  public static final String NEST_FROM_CONFIG_NAME = "nest.from";
  public static final String KEY_FIELD_NAMES_CONFIG_NAME = "key.field.names";
  public static final String NESTED_FIELD_NAME_CONFIG_NAME = "nested.field.name";

  private String intoDependency;
  private String fromDependency;
  private List<String> keyFieldNames;
  private String nestedFieldName;

  @Override
  public void configure(Config config) {
    this.intoDependency = config.getString(NEST_INTO_CONFIG_NAME);
    this.fromDependency = config.getString(NEST_FROM_CONFIG_NAME);
    this.keyFieldNames = config.getStringList(KEY_FIELD_NAMES_CONFIG_NAME);
    this.nestedFieldName = config.getString(NESTED_FIELD_NAME_CONFIG_NAME);
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    if (!dependencies.containsKey(intoDependency)) {
      throw new RuntimeException("Nest deriver points to non-existent nest-into dependency");
    }
    Dataset<Row> into = dependencies.get(intoDependency);

    if (!dependencies.containsKey(fromDependency)) {
      throw new RuntimeException("Nest deriver points to non-existent nest-from dependency");
    }
    Dataset<Row> from = dependencies.get(fromDependency);

    ExtractFieldsFunction extractFieldsFunction = new ExtractFieldsFunction(keyFieldNames);
    JavaPairRDD<List<Object>, Row> keyedIntoRDD = into.javaRDD().keyBy(extractFieldsFunction);
    JavaPairRDD<List<Object>, Row> keyedFromRDD = from.javaRDD().keyBy(extractFieldsFunction);

    NestFunction nestFunction = new NestFunction();
    JavaRDD<Row> nestedRDD = keyedIntoRDD.cogroup(keyedFromRDD).values().map(nestFunction);

    StructType nestedSchema = into.schema().add(nestedFieldName, DataTypes.createArrayType(from.schema()));

    Dataset<Row> nested = into.sqlContext().createDataFrame(nestedRDD, nestedSchema);

    return nested;
  }

  @Override
  public String getAlias() {
    return "nest";
  }

  @SuppressWarnings("serial")
  private static class ExtractFieldsFunction implements Function<Row, List<Object>> {
    private List<String> fieldNames;

    public ExtractFieldsFunction(List<String> fieldNames) {
      this.fieldNames = fieldNames;
    }

    @Override
    public List<Object> call(Row row) throws Exception {
      List<Object> values = new ArrayList<>();

      for (String fieldName : fieldNames) {
        values.add(row.get(row.fieldIndex(fieldName)));
      }

      return values;
    }
  }

  @SuppressWarnings("serial")
  private static class NestFunction implements Function<Tuple2<Iterable<Row>, Iterable<Row>>, Row> {
    @Override
    public Row call(Tuple2<Iterable<Row>, Iterable<Row>> cogrouped) throws Exception {
      // There should only be one 'into' record per key
      Row intoRow = cogrouped._1().iterator().next();
      Row[] fromRows = Iterables.toArray(cogrouped._2(), Row.class);
      int intoRowNumFields = intoRow.size();

      Object[] nestedValues = new Object[intoRowNumFields + 1];
      for (int i = 0; i < intoRowNumFields; i++) {
        nestedValues[i] = intoRow.get(i);
      }
      nestedValues[intoRowNumFields] = fromRows;

      Row nested = RowFactory.create(nestedValues);

      return nested;
    }
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(KEY_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(NESTED_FIELD_NAME_CONFIG_NAME, ConfigValueType.STRING)
        .mandatoryPath(NEST_FROM_CONFIG_NAME, ConfigValueType.STRING)
        .mandatoryPath(NEST_INTO_CONFIG_NAME, ConfigValueType.STRING)
        .build();
  }

}
