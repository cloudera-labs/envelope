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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.schema.AvroSchema;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ValidationAssert;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestParseJSONDeriver {

  @Test
  public void testDeriveAsFields() {
    Config config = ConfigFactory.empty()
        .withValue(ParseJSONDeriver.STEP_NAME_CONFIG, ConfigValueFactory.fromAnyRef("source"))
        .withValue(ParseJSONDeriver.FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(ParseJSONDeriver.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef(new AvroSchema().getAlias()))
        .withValue(ParseJSONDeriver.SCHEMA_CONFIG + "." + AvroSchema.AVRO_FILE_CONFIG,
            ConfigValueFactory.fromAnyRef(
                getClass().getResource("/json/json-schema.avsc").getFile()));

    Row row = deriveRow(config);

    assertEquals(6, row.length());
    assertEquals("hello", row.getAs("f1"));
    assertEquals(1.2f, row.getAs("f2"));
    assertEquals(true, row.getAs("f3"));
    assertEquals("world", row.getStruct(row.fieldIndex("f4")).getAs("f41"));
    assertEquals("v5", row.getList(row.fieldIndex("f5")).get(0));
    assertEquals(false, ((Row)row.getList(row.fieldIndex("f6")).get(0)).getList(0).get(0));
    assertEquals(false, ((Row)row.getList(row.fieldIndex("f6")).get(0)).getList(0).get(1));
  }

  @Test
  public void testDeriveAsStruct() {
    Config config = ConfigFactory.empty()
        .withValue(ParseJSONDeriver.STEP_NAME_CONFIG, ConfigValueFactory.fromAnyRef("source"))
        .withValue(ParseJSONDeriver.FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(ParseJSONDeriver.AS_STRUCT_CONFIG, ConfigValueFactory.fromAnyRef(true))
        .withValue(ParseJSONDeriver.STRUCT_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("parsed"))
        .withValue(ParseJSONDeriver.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef(new AvroSchema().getAlias()))
        .withValue(ParseJSONDeriver.SCHEMA_CONFIG + "." + AvroSchema.AVRO_FILE_CONFIG,
            ConfigValueFactory.fromAnyRef(
                getClass().getResource("/json/json-schema.avsc").getFile()));

    Row row = deriveRow(config);
    Row struct = row.getStruct(row.fieldIndex("parsed"));

    assertEquals(1, row.length());
    assertEquals(6, struct.length());
    assertEquals("hello", struct.getAs("f1"));
    assertEquals(1.2f, struct.getAs("f2"));
    assertEquals(true, struct.getAs("f3"));
    assertEquals("world", struct.getStruct(struct.fieldIndex("f4")).getAs("f41"));
    assertEquals("v5", struct.getList(struct.fieldIndex("f5")).get(0));
    assertEquals(false, ((Row)struct.getList(struct.fieldIndex("f6")).get(0)).getList(0).get(0));
    assertEquals(false, ((Row)struct.getList(struct.fieldIndex("f6")).get(0)).getList(0).get(1));
  }

  @Test (expected = Exception.class)
  public void testDeriveWithOptions() {
    Config config = ConfigFactory.empty()
        .withValue(ParseJSONDeriver.STEP_NAME_CONFIG, ConfigValueFactory.fromAnyRef("source"))
        .withValue(ParseJSONDeriver.FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(ParseJSONDeriver.AS_STRUCT_CONFIG, ConfigValueFactory.fromAnyRef(true))
        .withValue(ParseJSONDeriver.STRUCT_FIELD_NAME_CONFIG, ConfigValueFactory.fromAnyRef("parsed"))
        .withValue(ParseJSONDeriver.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef(new AvroSchema().getAlias()))
        .withValue(ParseJSONDeriver.SCHEMA_CONFIG + "." + AvroSchema.AVRO_FILE_CONFIG,
            ConfigValueFactory.fromAnyRef(
                getClass().getResource("/json/json-schema.avsc").getFile()))
        .withValue(ParseJSONDeriver.OPTION_CONFIG_PREFIX + "." + "allowSingleQuotes",
            ConfigValueFactory.fromAnyRef(false))
        .withValue(ParseJSONDeriver.OPTION_CONFIG_PREFIX + "." + "mode",
            ConfigValueFactory.fromAnyRef("FAILFAST"));

    Row row = deriveRow(config);
    row.getStruct(row.fieldIndex("parsed")).getAs("f1");
  }

  private Row deriveRow(Config deriverConfig) {
    ParseJSONDeriver deriver = new ParseJSONDeriver();
    ValidationAssert.assertNoValidationFailures(deriver, deriverConfig);
    deriver.configure(deriverConfig);

    String json = "{\n" +
        "  \"f1\": \'hello\',\n" +
        "  \"f2\": 1.2,\n" +
        "  \"f3\": true,\n" +
        "  \"f4\": {\n" +
        "    \"f41\": \'world\'\n" +
        "  },\n" +
        "  \"f5\": [\n" +
        "    \"v5\"\n" +
        "  ],\n" +
        "  \"f6\": [\n" +
        "    {\n" +
        "      \"f611\": [false, false]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    Row message = new RowWithSchema(SchemaUtils.stringValueSchema(), json);
    Dataset<Row> dependency = Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(message), message.schema());
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("source", dependency);

    List<Row> derivedRows = deriver.derive(dependencies).collectAsList();

    assertEquals(1, derivedRows.size());

    return derivedRows.iterator().next();
  }

}
