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

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.schema.FlatSchema;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.List;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;

public class TestDelimitedTranslator {
  @Test
  public void testTranslation() throws Exception {
    String delimited = "hello%$-100.1%$1000.5%$99%$888%$%$false%$2018-03-03T20:23:33.897+04:00";
    Config config = ConfigFactory.empty()
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef("flat"))
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG, 
            ConfigValueFactory.fromIterable(
                Lists.newArrayList("field1", "field2", "field3", "field4", 
                                   "field5", "field6", "field7", "field8")))
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
            ConfigValueFactory.fromIterable(
                Lists.newArrayList("string", "float", "double", "int", 
                                   "long", "int", "boolean", "timestamp")))
        .withValue(DelimitedTranslator.DELIMITER_CONFIG_NAME, ConfigValueFactory.fromAnyRef("%$"));

    Translator t = new DelimitedTranslator();
    assertNoValidationFailures((DelimitedTranslator)t, config);
    t.configure(config);
    Row raw = TestingMessageFactory.get(delimited, DataTypes.StringType);
    Iterable<Row> ir = t.translate(raw);
    
    Row r = t.translate(raw).iterator().next();
    assertEquals(r.length(), 8);
    assertEquals(r.get(0), "hello");
    assertEquals(r.get(1), -100.1f);
    assertEquals(r.get(2), 1000.5d);
    assertEquals(r.get(3), 99);
    assertEquals(r.get(4), 888L);
    assertEquals(r.get(5), null);
    assertEquals(r.get(6), false);
    assertEquals(((Timestamp)r.get(7)).getTime(), 1520094213897L);
  }

  @Test
  public void testEmptyFields() {
    String delimited1 = "000001|2017-11-01 23:21:21.924||TYPE|DATA";
    String delimited2 = "000002|2017-11-01 23:21:21.924|101|TYPE|";

    List<String> fieldNames = Lists.newArrayList(
        "event_id", "event_time", "event_state", "event_type", "event_data");
    List<String> fieldTypes = Lists.newArrayList("long", "string", "long", "string", "string");

    Config config = ConfigFactory.empty()
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef("flat"))
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
            ConfigValueFactory.fromIterable(fieldNames))
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
            ConfigValueFactory.fromIterable(fieldTypes))
        .withValue(DelimitedTranslator.DELIMITER_CONFIG_NAME, ConfigValueFactory.fromAnyRef("|"));

    DelimitedTranslator t = new DelimitedTranslator();
    assertNoValidationFailures(t, config);
    t.configure(config);

    Row raw1 = TestingMessageFactory.get("testkey1", DataTypes.StringType, delimited1, DataTypes.StringType);
    Row r1 = t.translate(raw1).iterator().next();
    assertEquals(r1.length(), 5);
    assertEquals(r1.get(0), 000001L);
    assertEquals(r1.get(1), "2017-11-01 23:21:21.924");
    assertEquals(r1.get(2), null);
    assertEquals(r1.get(3), "TYPE");
    assertEquals(r1.get(4), "DATA");

    Row raw2 = TestingMessageFactory.get("testkey2", DataTypes.StringType, delimited2, DataTypes.StringType);
    Row r2 = t.translate(raw2).iterator().next();
    assertEquals(r2.length(), 5);
    assertEquals(r2.get(0), 000002L);
    assertEquals(r2.get(1), "2017-11-01 23:21:21.924");
    assertEquals(r2.get(2), 101L);
    assertEquals(r2.get(3), "TYPE");
    assertEquals(r2.get(4), null);
  }

  @Test
  public void testNullMissing() {
    String delimited = "val1 2 34";
    
    Config config = ConfigFactory.empty()
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef("flat"))
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
            ConfigValueFactory.fromIterable(
                Lists.newArrayList("field1", "field2", "field3", "field4", "field5")))
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
            ConfigValueFactory.fromIterable(
                Lists.newArrayList("string", "int", "long", "int", "boolean")))
        .withValue(DelimitedTranslator.DELIMITER_CONFIG_NAME, ConfigValueFactory.fromAnyRef(" "));

    DelimitedTranslator t = new DelimitedTranslator();
    assertNoValidationFailures(t, config);
    t.configure(config);
    Row raw = TestingMessageFactory.get("testkey", DataTypes.StringType, delimited, DataTypes.StringType);
    Row r = t.translate(raw).iterator().next();
    assertEquals(r.length(), 5);
    assertEquals(r.get(0), "val1");
    assertEquals(r.get(1), 2);
    assertEquals(r.get(2), 34L);
    assertEquals(r.get(3), null);
    assertEquals(r.get(4), null);
  }

  @Test
  public void testRegexDelimiter() {
    String delimited = "val1 \"val2 ...\" val3 \"val4 val5\"";
    
    Config config = ConfigFactory.empty()
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef("flat"))
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG,
            ConfigValueFactory.fromIterable(
                Lists.newArrayList("field1", "field2", "field3", "field4")))
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
            ConfigValueFactory.fromIterable(
                Lists.newArrayList("string", "string", "string", "string")))
        .withValue(DelimitedTranslator.DELIMITER_CONFIG_NAME, 
            ConfigValueFactory.fromAnyRef(" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
        .withValue(DelimitedTranslator.DELIMITER_REGEX_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef(true));

    DelimitedTranslator t = new DelimitedTranslator();
    assertNoValidationFailures(t, config);
    t.configure(config);
    Row raw = TestingMessageFactory.get("testkey", DataTypes.StringType, delimited, DataTypes.StringType);
    Row r = t.translate(raw).iterator().next();
    assertEquals(r.length(), 4);
    assertEquals(r.get(0), "val1");
    assertEquals(r.get(1), "\"val2 ...\"");
    assertEquals(r.get(2), "val3");
    assertEquals(r.get(3), "\"val4 val5\"");
  }

  @Test
  public void testTimestampFormats() {
    String delimited = "2018-09-19 23:49:29.92284%$2018-09-09 23:49:29.00000%$1000.5%$99%$888%$%$false%$2018-09-19 00:00:00";

    Config config = ConfigFactory.empty()
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + ComponentFactory.TYPE_CONFIG_NAME,
            ConfigValueFactory.fromAnyRef("flat"))
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_NAMES_CONFIG, 
            ConfigValueFactory.fromIterable(
                Lists.newArrayList("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8")))
        .withValue(DelimitedTranslator.SCHEMA_CONFIG + "." + FlatSchema.FIELD_TYPES_CONFIG,
            ConfigValueFactory.fromIterable(
                Lists.newArrayList("timestamp", "timestamp", "double", "int", "long", "int", "boolean", "timestamp")))
        .withValue(DelimitedTranslator.DELIMITER_CONFIG_NAME, ConfigValueFactory.fromAnyRef("%$"))
        .withValue(DelimitedTranslator.TIMESTAMP_FORMAT_CONFIG_NAME, ConfigValueFactory.fromIterable(
            Lists.newArrayList("yyyy-MM-dd HH:mm:ss.SSSSS", "yyyy-MM-dd HH:mm:ss")));

    DelimitedTranslator t = new DelimitedTranslator();
    assertNoValidationFailures(t, config);
    t.configure(config);
    Row raw = TestingMessageFactory.get(delimited, DataTypes.StringType);
    Row r = t.translate(raw).iterator().next();
    assertEquals(r.length(), 8);
    // Timestamp microseconds to miliseconds truncation
    assertEquals(new LocalDateTime(r.get(0)).
        toDateTime(DateTimeZone.UTC).toString(), "2018-09-19T23:49:29.922Z");
    assertEquals(new LocalDateTime(r.get(1)).
        toDateTime(DateTimeZone.UTC).toString(), "2018-09-09T23:49:29.000Z");
    assertEquals(r.get(2), 1000.5d);
    assertEquals(r.get(3), 99);
    assertEquals(r.get(4), 888L);
    assertEquals(r.get(5), null);
    assertEquals(r.get(6), false);
    assertEquals(new LocalDateTime(r.get(7)).
        toDateTime(DateTimeZone.UTC).toString(), "2018-09-19T00:00:00.000Z");
  }

}
