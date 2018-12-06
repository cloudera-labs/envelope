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

package com.cloudera.labs.envelope.translate;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.schema.UsesProvidedSchema;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;

public class RawTranslator implements Translator, ProvidesAlias, UsesProvidedSchema {

  private StructType schema;

  @Override
  public void configure(Config config) { }

  @Override
  public Iterable<Row> translate(Row message) {
    List<Object> values = Lists.newArrayList();

    for (StructField field : message.schema().fields()) {
      values.add(RowUtils.get(message, field.name()));
    }

    Row row = new RowWithSchema(message.schema(), values.toArray());

    return Collections.singleton(row);
  }

  @Override
  public StructType getExpectingSchema() {
    // The raw translator doesn't expect anything specific
    return DataTypes.createStructType(Lists.<StructField>newArrayList());
  }

  @Override
  public void receiveProvidedSchema(StructType providedSchema) {
    this.schema = providedSchema;
  }

  @Override
  public StructType getProvidingSchema() {
    return schema;
  }

  @Override
  public String getAlias() {
    return "raw";
  }

}
