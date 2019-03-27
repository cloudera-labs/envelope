/*
 * Copyright (c) 2015-2019, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.labs.envelope.schema;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.utils.AvroUtils;
import com.cloudera.labs.envelope.validate.AvroSchemaLiteralValidation;
import com.cloudera.labs.envelope.validate.AvroSchemaPathValidation;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import java.io.File;
import java.io.IOException;
import org.apache.spark.sql.types.StructType;

public class AvroSchema implements Schema, ProvidesAlias, ProvidesValidations {

  public static final String AVRO_LITERAL_CONFIG = "literal";
  public static final String AVRO_FILE_CONFIG = "filepath";

  private StructType schema;

  @Override
  public void configure(Config config) {
    org.apache.avro.Schema avroSchema;
    if (config.hasPath(AVRO_FILE_CONFIG)) {
      try {
        File avroFile = new File(config.getString(AVRO_FILE_CONFIG));
        avroSchema = new org.apache.avro.Schema.Parser().parse(avroFile);
      } catch (IOException e) {
        throw new RuntimeException("Error parsing Avro schema file", e);
      }
    } else {
      avroSchema = new org.apache.avro.Schema.Parser().parse(config.getString(AVRO_LITERAL_CONFIG));
    }
    this.schema = AvroUtils.structTypeFor(avroSchema);
  }

  @Override
  public String getAlias() {
    return "avro";
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .exactlyOnePathExists(ConfigValueType.STRING, AVRO_FILE_CONFIG, AVRO_LITERAL_CONFIG)
        .ifPathExists(AVRO_FILE_CONFIG, new AvroSchemaPathValidation(AVRO_FILE_CONFIG))
        .ifPathExists(AVRO_LITERAL_CONFIG, new AvroSchemaLiteralValidation(AVRO_LITERAL_CONFIG))
        .build();
  }
  
}
