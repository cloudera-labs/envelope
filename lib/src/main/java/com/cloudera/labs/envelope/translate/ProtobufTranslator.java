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

package com.cloudera.labs.envelope.translate;

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.schema.ProtobufSchema;
import com.cloudera.labs.envelope.schema.Schema;
import com.cloudera.labs.envelope.schema.SchemaFactory;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.ProtobufUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validation;
import com.cloudera.labs.envelope.validate.Validations;
import com.cloudera.labs.envelope.validate.ValidationResult;
import com.cloudera.labs.envelope.validate.Validity;
import com.google.common.collect.Sets;
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
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * Convert proto3 messages into Rows.
 * <p>
 * This supports both compressed (gzip) or uncompressed message payloads.
 */
public class ProtobufTranslator implements Translator, ProvidesAlias, ProvidesValidations,
    InstantiatesComponents {

  private static final Logger LOG = LoggerFactory.getLogger(ProtobufTranslator.class);

  private Descriptors.Descriptor descriptor;
  private StructType schema;

  public static final String SCHEMA_CONFIG = "schema";

  @Override
  public Iterable<Row> translate(Row message) throws Exception {
    byte[] value = message.getAs(Translator.VALUE_FIELD_NAME);

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

    // Validations ensure schema is of type protobuf
    ProtobufSchema protobufSchema = (ProtobufSchema)SchemaFactory.create(config.getConfig(SCHEMA_CONFIG), true);
    this.descriptor = protobufSchema.getDescriptor();
    this.schema = protobufSchema.getSchema();
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(SCHEMA_CONFIG, ConfigValueType.OBJECT)
        .ifPathExists(SCHEMA_CONFIG, new ProtobufTranslatorSchemaTypeValidation(SCHEMA_CONFIG))
        .handlesOwnValidationPath(SCHEMA_CONFIG)
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    return SchemaUtils.getSchemaComponents(config, configure, SCHEMA_CONFIG);
  }

  private static class ProtobufTranslatorSchemaTypeValidation implements Validation {
    private String path;

    public ProtobufTranslatorSchemaTypeValidation(String path) {
      this.path = path;
    }
 
    @Override
    public ValidationResult validate(Config config) {
      Schema schema = SchemaFactory.create(config, true);
      if (!(schema instanceof ProtobufSchema)) { 
        return new ValidationResult(this, Validity.INVALID,
            "Protobuf translator schema can only be of type 'protobuf'.  " +
                "See stack trace below for more information.");
      }
      return new ValidationResult(this, Validity.VALID, "Protobuf translator schema is of type 'protobuf.'");
    }

    @Override
    public Set<String> getKnownPaths() {
      return Sets.newHashSet(path);
    }
  }

}
