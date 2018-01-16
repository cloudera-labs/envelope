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
package com.cloudera.labs.envelope.input.translate;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.spark.sql.Row;
import org.junit.Test;

import com.cloudera.labs.envelope.utils.TranslatorUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestDelimitedTranslator {

  @Test
  public void testTranslation() throws Exception {
    String delimited = "hello%$-100.1%$1000.5%$99%$888%$%$false";
    
    Config config = ConfigFactory.empty()
        .withValue(DelimitedTranslator.FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromIterable(
            Lists.newArrayList("field1", "field2", "field3", "field4", "field5", "field6", "field7")))
        .withValue(DelimitedTranslator.FIELD_TYPES_CONFIG_NAME, ConfigValueFactory.fromIterable(
            Lists.newArrayList("string", "float", "double", "int", "long", "int", "boolean")))
        .withValue(DelimitedTranslator.DELIMITER_CONFIG_NAME, ConfigValueFactory.fromAnyRef("%$"));
    
    Translator<String, String> t = new DelimitedTranslator();
    t.configure(config);
    
    Row r = t.translate(null, delimited).iterator().next();
    
    assertEquals(r.length(), 7);
    assertEquals(r.get(0), "hello");
    assertEquals(r.get(1), -100.1f);
    assertEquals(r.get(2), 1000.5d);
    assertEquals(r.get(3), 99);
    assertEquals(r.get(4), 888L);
    assertEquals(r.get(5), null);
    assertEquals(r.get(6), false);
  }
  
  @Test
  public void testAppendRaw() throws Exception {
    String delimited = "hello%$-100.1%$1000.5%$99%$888%$%$false";
    
    Config config = ConfigFactory.empty()
        .withValue(DelimitedTranslator.FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromIterable(
            Lists.newArrayList("field1", "field2", "field3", "field4", "field5", "field6", "field7")))
        .withValue(DelimitedTranslator.FIELD_TYPES_CONFIG_NAME, ConfigValueFactory.fromIterable(
            Lists.newArrayList("string", "float", "double", "int", "long", "int", "boolean")))
        .withValue(DelimitedTranslator.DELIMITER_CONFIG_NAME, ConfigValueFactory.fromAnyRef("%$"))
        .withValue(TranslatorUtils.APPEND_RAW_ENABLED_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true));
    
    Translator<String, String> t = new DelimitedTranslator();
    t.configure(config);
    
    Row r = t.translate("testkey", delimited).iterator().next();
    
    assertEquals(r.length(), 9);
    assertEquals(r.get(0), "hello");
    assertEquals(r.get(1), -100.1f);
    assertEquals(r.get(2), 1000.5d);
    assertEquals(r.get(3), 99);
    assertEquals(r.get(4), 888L);
    assertEquals(r.get(5), null);
    assertEquals(r.get(6), false);
    assertEquals(r.get(7), "testkey");
    assertEquals(r.get(8), delimited);
  }

  @Test
  public void testEmptyFields() throws Exception {
    String delimited1 = "000001|2017-11-01 23:21:21.924||TYPE|DATA";
    String delimited2 = "000002|2017-11-01 23:21:21.924|101|TYPE|";

    List<String> fieldNames = Lists.newArrayList("event_id", "event_time", "event_state", "event_type", "event_data");
    List<String> fieldTypes = Lists.newArrayList("long", "string", "long", "string", "string");

    Config config = ConfigFactory.empty()
            .withValue(DelimitedTranslator.FIELD_NAMES_CONFIG_NAME, ConfigValueFactory.fromIterable(fieldNames))
            .withValue(DelimitedTranslator.FIELD_TYPES_CONFIG_NAME, ConfigValueFactory.fromIterable(fieldTypes))
            .withValue(DelimitedTranslator.DELIMITER_CONFIG_NAME, ConfigValueFactory.fromAnyRef("|"));

    Translator<String, String> t = new DelimitedTranslator();
    t.configure(config);

    Row r1 = t.translate("testkey1", delimited1).iterator().next();
    assertEquals(r1.length(), 5);
    assertEquals(r1.get(0), 000001L);
    assertEquals(r1.get(1), "2017-11-01 23:21:21.924");
    assertEquals(r1.get(2), null);
    assertEquals(r1.get(3), "TYPE");
    assertEquals(r1.get(4), "DATA");

    Row r2 = t.translate("testkey2", delimited2).iterator().next();
    assertEquals(r2.length(), 5);
    assertEquals(r2.get(0), 000002L);
    assertEquals(r2.get(1), "2017-11-01 23:21:21.924");
    assertEquals(r2.get(2), 101L);
    assertEquals(r2.get(3), "TYPE");
    assertEquals(r2.get(4), null);
  }
  
}
