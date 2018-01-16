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
package com.cloudera.labs.envelope.kafka.serde;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.base.Joiner;

public class DelimitedSerializer implements Serializer<Row> {

  public static final String FIELD_DELIMITER_CONFIG_NAME = "field.delimiter";
  
  private StringSerializer stringSerializer;
  private Joiner joiner;
  
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    stringSerializer = new StringSerializer();
    
    String delimiter = configs.get(FIELD_DELIMITER_CONFIG_NAME).toString();
    joiner = Joiner.on(delimiter);
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
