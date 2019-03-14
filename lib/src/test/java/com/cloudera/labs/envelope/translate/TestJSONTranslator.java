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

import com.cloudera.labs.envelope.schema.AvroSchema;
import com.cloudera.labs.envelope.schema.SchemaFactory;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ValidationAssert;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestJSONTranslator {

  @Test
  public void testTranslation() {
    Config config = ConfigFactory.empty()
        .withValue(JSONTranslator.SCHEMA_CONFIG + "." + SchemaFactory.TYPE_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef(new AvroSchema().getAlias()))
        .withValue(JSONTranslator.SCHEMA_CONFIG + "." + AvroSchema.AVRO_FILE_CONFIG,
            ConfigValueFactory.fromAnyRef(
                getClass().getResource("/translator/json-translator-schema.avsc").getFile()));

    JSONTranslator translator = new JSONTranslator();
    ValidationAssert.assertNoValidationFailures(translator, config);
    translator.configure(config);

    String json = "{\n" +
        "  \"f1\": \"hello\",\n" +
        "  \"f2\": 1.2,\n" +
        "  \"f3\": true,\n" +
        "  \"f4\": {\n" +
        "    \"f41\": \"world\"\n" +
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

    Iterable<Row> rows = translator.translate(message);

    assertEquals(1, Lists.newArrayList(rows).size());
    Row row = rows.iterator().next();

    assertEquals("hello", row.getAs("f1"));
    assertEquals(1.2f, row.getAs("f2"));
    assertEquals(true, row.getAs("f3"));
    assertEquals("world", row.getStruct(row.fieldIndex("f4")).getAs("f41"));
    assertEquals("v5", row.getList(row.fieldIndex("f5")).get(0));
    assertEquals(false, ((Row)row.getList(row.fieldIndex("f6")).get(0)).getList(0).get(0));
    assertEquals(false, ((Row)row.getList(row.fieldIndex("f6")).get(0)).getList(0).get(1));
  }

}
