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
import com.cloudera.labs.envelope.utils.ProtobufUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.protobuf.Descriptors;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtobufSchema implements Schema, ProvidesAlias, ProvidesValidations {

  private static final Logger LOG = LoggerFactory.getLogger(ProtobufSchema.class);

  public static final String DESCRIPTOR_FILEPATH_CONFIG = "descriptor.filepath";
  public static final String DESCRIPTOR_MESSAGE_CONFIG = "descriptor.message";
  public static final String ALIAS = "protobuf";

  private Descriptors.Descriptor descriptor;
  private StructType schema;

  @Override
  public void configure(Config config) {
    String descriptorFilePath = config.getString(DESCRIPTOR_FILEPATH_CONFIG);

    if (config.hasPath(DESCRIPTOR_MESSAGE_CONFIG)) {
      String descriptorMessage = config.getString(DESCRIPTOR_MESSAGE_CONFIG);
      this.descriptor = ProtobufUtils.buildDescriptor(descriptorFilePath, descriptorMessage);
    } else {
      this.descriptor = ProtobufUtils.buildDescriptor(descriptorFilePath);
    }

    this.schema = ProtobufUtils.buildSchema(descriptor);
  }

  @Override
  public String getAlias() {
    return ALIAS;
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(DESCRIPTOR_FILEPATH_CONFIG, ConfigValueType.STRING)
        .optionalPath(DESCRIPTOR_MESSAGE_CONFIG, ConfigValueType.STRING)
        .build();
  }
  
  public Descriptors.Descriptor getDescriptor() {
    return descriptor;
  }
}
