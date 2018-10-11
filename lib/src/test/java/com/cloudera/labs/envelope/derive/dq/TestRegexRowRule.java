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

public class TestRegexRowRule {

  private static final StructType SCHEMA = new StructType(new StructField[] {
      new StructField("name", DataTypes.StringType, false, Metadata.empty()),
      new StructField("nickname", DataTypes.StringType, false, Metadata.empty()),
      new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("candycrushscore", DataTypes.createDecimalType(), false, Metadata.empty())
  });

  @Test
  public void testNameRegex() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("fields", Lists.newArrayList("name", "nickname"));
    configMap.put("regex", "[a-zA-Z ]{1,}");
    Config config = ConfigFactory.parseMap(configMap);

    RegexRowRule rule = new RegexRowRule();
    assertNoValidationFailures(rule, config);
    rule.configure("namecheck", config);

    Row row1 = new RowWithSchema(SCHEMA, "Ian", "Ian", 34, new BigDecimal("0.00"));
    assertTrue("Row should pass rule", rule.check(row1));

    Row row2 = new RowWithSchema(SCHEMA, "Webster1", "Websta1", 110, new BigDecimal("450.10"));
    assertFalse("Row should not pass rule", rule.check(row2));

    Row row3 = new RowWithSchema(SCHEMA, "", "Ian1", 110, new BigDecimal("450.10"));
    assertFalse("Row should not pass rule", rule.check(row3));

    Row row4 = new RowWithSchema(SCHEMA, "First Last", "Ian Last", 110, new BigDecimal("450.10"));
    assertTrue("Row should pass rule", rule.check(row4));
  }

}
