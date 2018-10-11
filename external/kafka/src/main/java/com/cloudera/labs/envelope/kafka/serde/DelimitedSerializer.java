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

package com.cloudera.labs.envelope.kafka.serde;

import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.base.Joiner;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Row;

import java.util.Map;

public class DelimitedSerializer implements Serializer<Row> {

  public static final String FIELD_DELIMITER_CONFIG_NAME = "field.delimiter";
  public static final String USE_FOR_NULL_CONFIG_NAME = "use.for.null";
  public static final String USE_FOR_NULL_DEFAULT_VALUE = "";
  
  private StringSerializer stringSerializer;
  private Joiner joiner;
  
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    stringSerializer = new StringSerializer();
    
    String delimiter = configs.get(FIELD_DELIMITER_CONFIG_NAME).toString();
    String useForNull;
    if (configs.containsKey(USE_FOR_NULL_CONFIG_NAME)) {
      useForNull = configs.get(USE_FOR_NULL_CONFIG_NAME).toString();
    } else {
      useForNull = USE_FOR_NULL_DEFAULT_VALUE;
    }
    joiner = Joiner.on(delimiter).useForNull(useForNull);
  }

  @Override
  public byte[] serialize(String topic, Row data) {
    if (data == null) {
      return null;
    }
    
    String message = joiner.join(RowUtils.valuesFor(data));
    
    return stringSerializer.serialize(null, message);
  }

  @Override
  public void close() {
    // Nothing to do
  }

}
