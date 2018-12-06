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
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.ProtobufUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Convert proto3 messages into Rows.
 * <p>
 * This supports both compressed (gzip) or uncompressed message payloads.
 */
public class ProtobufTranslator implements Translator, ProvidesAlias, ProvidesValidations {

  private static final Logger LOG = LoggerFactory.getLogger(ProtobufTranslator.class);

  public static final String CONFIG_DESCRIPTOR_FILEPATH = "descriptor.filepath";
  public static final String CONFIG_DESCRIPTOR_MESSAGE = "descriptor.message";

  private Descriptors.Descriptor descriptor;
  private StructType schema;

  @Override
  public Iterable<Row> translate(Row message) throws Exception {
    byte[] value = RowUtils.get(message, Translator.VALUE_FIELD_NAME);

    if (value == null || value.length < 1) {
      throw new RuntimeException("Payload value is null or empty");
    }

    DynamicMessage msg;

    BufferedInputStream valueInputStream = new BufferedInputStream(new ByteArrayInputStream(value));

    // Parse into Message
    try {
      // Check if the value is gzipped
      if (ProtobufUtils.isGzipped(valueInputStream)) {
        LOG.trace("Decompressing GZIP byte array");
        msg = DynamicMessage.parseFrom(descriptor, new GZIPInputStream(valueInputStream));
      } else {
        msg = DynamicMessage.parseFrom(descriptor, valueInputStream);
      }
    } catch (InvalidProtocolBufferException ex) {
      throw new RuntimeException("Error while parsing message from descriptor and raw bytes", ex);
    }

    // Populate set of row values matching full schema
    // NOTE: very likely this will be sparse
    List<Object> rowValues = ProtobufUtils.buildRowValues(descriptor, msg, schema);
    Row row = new RowWithSchema(schema, rowValues.toArray());

    return Collections.singletonList(row);
  }

  public String getAlias() {
    return "protobuf";
  }

  @Override
  public StructType getExpectingSchema() {
    return SchemaUtils.binaryValueSchema();
  }

  @Override
  public StructType getProvidingSchema() {
    return schema;
  }

  @Override
  public void configure(Config config) {
    LOG.debug("Configuring ProtobufTranslator");

    String descriptorFilePath = config.getString(CONFIG_DESCRIPTOR_FILEPATH);

    if (config.hasPath(CONFIG_DESCRIPTOR_MESSAGE)) {
      String descriptorMessage = config.getString(CONFIG_DESCRIPTOR_MESSAGE);
      this.descriptor = ProtobufUtils.buildDescriptor(descriptorFilePath, descriptorMessage);
    } else {
      this.descriptor = ProtobufUtils.buildDescriptor(descriptorFilePath);
    }

    // Build full schema
    this.schema = ProtobufUtils.buildSchema(descriptor);
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(CONFIG_DESCRIPTOR_FILEPATH, ConfigValueType.STRING)
        .optionalPath(CONFIG_DESCRIPTOR_MESSAGE, ConfigValueType.STRING)
        .build();
  }
}