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
import com.cloudera.labs.envelope.schema.SchemaFactory;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.AvroUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * A translator implementation for binary Apache Avro generic record messages.
 */
public class AvroTranslator implements Translator, ProvidesAlias, ProvidesValidations, 
    InstantiatesComponents  {

  private StructType schema;
  private GenericDatumReader<GenericRecord> reader;

  public static final String SCHEMA_CONFIG = "schema";

  @Override
  public void configure(Config config) {
    schema = SchemaFactory.create(config.getConfig(SCHEMA_CONFIG), true).getSchema();
    reader = new GenericDatumReader<>(AvroUtils.schemaFor(schema));
  }

  @Override
  public Iterable<Row> translate(Row message) throws Exception {
    byte[] value = message.getAs(Translator.VALUE_FIELD_NAME);

    Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
    GenericRecord record = reader.read(null, decoder);
    Row row = rowForRecord(record);

    return Collections.singleton(row);
  }

  @Override
  public StructType getExpectingSchema() {
    return SchemaUtils.binaryValueSchema();
  }

  @Override
  public StructType getProvidingSchema() {
    return schema;
  }

  private Row rowForRecord(GenericRecord record) {
    List<Object> values = Lists.newArrayList();

    for (Field field : record.getSchema().getFields()) {
      Object value = record.get(field.name());

      Type fieldType = field.schema().getType();
      if (fieldType.equals(Type.UNION)) {
        fieldType = field.schema().getTypes().get(1).getType();
      }
      // Avro returns Utf8s for strings, which Spark SQL doesn't know how to use
      if (fieldType.equals(Type.STRING) && value != null) {
        value = value.toString();
      }

      values.add(value);
    }

    return new RowWithSchema(schema, values.toArray());
  }

  @Override
  public String getAlias() {
    return "avro";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(SCHEMA_CONFIG, ConfigValueType.OBJECT)
        .handlesOwnValidationPath(SCHEMA_CONFIG)
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    return SchemaUtils.getSchemaComponents(config, configure, SCHEMA_CONFIG);
  }
  
}
