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

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;

public class DummyInputFormatTranslator implements Translator {

  @Override
  public void configure(Config config) {}

  @Override
  public Iterable<Row> translate(Row message) {
    return Collections.singletonList(message);
  }

  @Override
  public StructType getExpectingSchema() {
    return DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", DataTypes.LongType, false),
        DataTypes.createStructField("value", DataTypes.StringType, false)
    ));
  }

  @Override
  public StructType getProvidingSchema() {
    return getExpectingSchema();
  }

}

