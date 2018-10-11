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

package com.cloudera.labs.envelope.derive.dq;


import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestEnumRowRule {

  private static final StructType SCHEMA = new StructType(new StructField[] {
      new StructField("name", DataTypes.StringType, false, Metadata.empty()),
      new StructField("nickname", DataTypes.StringType, false, Metadata.empty()),
      new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("candycrushscore", DataTypes.createDecimalType(), false, Metadata.empty())
  });

  @Test
  public void testStringEnums() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("fields", Lists.newArrayList("name", "nickname"));
    configMap.put("fieldtype", "string");
    configMap.put("values", Lists.newArrayList("Ian", "Jeremy", "Webster"));
    Config config = ConfigFactory.parseMap(configMap);

    EnumRowRule rule = new EnumRowRule();
    assertNoValidationFailures(rule, config);
    rule.configure("namecheck", config);

    Row row1 = new RowWithSchema(SCHEMA, "Ian", "Ian", 34, new BigDecimal("0.00"));
    assertTrue("Row should pass rule", rule.check(row1));

    Row row2 = new RowWithSchema(SCHEMA, "Webster", "Websta", 110, new BigDecimal("450.10"));
    assertFalse("Row should not pass rule", rule.check(row2));
  }

  @Test
  public void testStringEnumsCaseInsensitive() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("fields", Lists.newArrayList("name", "nickname"));
    configMap.put("fieldtype", "string");
    configMap.put("values", Lists.newArrayList("Ian", "Jeremy", "Webster"));
    configMap.put("case-sensitive", false);
    Config config = ConfigFactory.parseMap(configMap);

    EnumRowRule rule = new EnumRowRule();
    assertNoValidationFailures(rule, config);
    rule.configure("namecheck", config);

    Row row1 = new RowWithSchema(SCHEMA, "Ian", "ian", 34, new BigDecimal("0.00"));
    assertTrue("Row should pass rule", rule.check(row1));

    Row row2 = new RowWithSchema(SCHEMA, "Webster", "Websta", 110, new BigDecimal("450.10"));
    assertFalse("Row should not pass rule", rule.check(row2));
  }

  @Test
  public void testIntEnums() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("fields", Lists.newArrayList("age"));
    configMap.put("fieldtype", "int");
    configMap.put("values", Lists.newArrayList(34, 42, 111));
    Config config = ConfigFactory.parseMap(configMap);

    EnumRowRule rule = new EnumRowRule();
    assertNoValidationFailures(rule, config);
    rule.configure("agecheck", config);

    Row row1 = new RowWithSchema(SCHEMA, "Ian", "Ian", 34, new BigDecimal("0.00"));
    assertTrue("Row should pass rule", rule.check(row1));

    Row row2 = new RowWithSchema(SCHEMA, "Webster", "Websta", 110, new BigDecimal("450.10"));
    assertFalse("Row should not pass rule", rule.check(row2));
  }

  @Test
  public void testLongEnums() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("fields", Lists.newArrayList("age"));
    configMap.put("fieldtype", "long");
    configMap.put("values", Lists.newArrayList(34L, 42L, 111L));
    Config config = ConfigFactory.parseMap(configMap);

    EnumRowRule rule = new EnumRowRule();
    assertNoValidationFailures(rule, config);
    rule.configure("scorecheck", config);

    Row row1 = new RowWithSchema(SCHEMA, "Ian", "Ian", 34L, new BigDecimal("0.00"));
    assertTrue("Row should pass rule", rule.check(row1));

    Row row2 = new RowWithSchema(SCHEMA, "Webster", "Websta", 110L, new BigDecimal("450.10"));
    assertFalse("Row should not pass rule", rule.check(row2));
  }

  @Test
  public void testDecimalEnums() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("fields", Lists.newArrayList("candycrushscore"));
    configMap.put("fieldtype", "decimal");
    configMap.put("values", Lists.newArrayList("0.01", "450.10"));
    Config config = ConfigFactory.parseMap(configMap);

    EnumRowRule rule = new EnumRowRule();
    assertNoValidationFailures(rule, config);
    rule.configure("scorecheck", config);

    Row row1 = new RowWithSchema(SCHEMA, "Ian", "Ian", 34, new BigDecimal("0.00"));
    assertFalse("Row should not pass rule", rule.check(row1));

    Row row2 = new RowWithSchema(SCHEMA, "Webster", "Websta", 110, new BigDecimal("450.10"));
    assertTrue("Row should pass rule", rule.check(row2));
  }

}
