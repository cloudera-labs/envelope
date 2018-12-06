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

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class TestingMessageFactory {

  public static Row get(Object value, DataType valueDataType) {
    return get(null, DataTypes.BinaryType, value, valueDataType);
  }

  public static Row get(Object key, DataType keyDataType, Object value, DataType valueDataType) {
    return new RowWithSchema(DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("key", keyDataType, false),
        DataTypes.createStructField("value", valueDataType, false)
    )), key, value);
  }

}
