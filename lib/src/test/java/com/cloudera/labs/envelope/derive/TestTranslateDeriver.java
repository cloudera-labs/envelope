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

import com.cloudera.labs.envelope.schema.ConfigurationDataTypes;
import com.cloudera.labs.envelope.schema.FlatSchema;
import com.cloudera.labs.envelope.schema.SchemaFactory;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.translate.DelimitedTranslator;
import com.cloudera.labs.envelope.translate.TranslatorFactory;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.validate.ValidationAssert;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestTranslateDeriver {

  @Test
  public void testTranslate() {
    Row good = RowFactory.create("abc", true, "hello:world:1", -1.0);
    Row bad = RowFactory.create("def", false, "hello:world:!", 1.0);

    Dataset<Row> step = Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(good, bad),
        DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("a", DataTypes.StringType, false),
            DataTypes.createStructField("b", DataTypes.BooleanType, false),
            DataTypes.createStructField("c", DataTypes.StringType, false),
            DataTypes.createStructField("d", DataTypes.DoubleType, false)
        ))
    );
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("step_name", step);

    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(TranslateDeriver.STEP_CONFIG, "step_name");
    configMap.put(TranslateDeriver.FIELD_CONFIG, "c");
    configMap.put(TranslateDeriver.TRANSLATOR_CONFIG + "." + TranslatorFactory.TYPE_CONFIG_NAME,
        "delimited");
    configMap.put(TranslateDeriver.TRANSLATOR_CONFIG + "." + DelimitedTranslator.DELIMITER_CONFIG_NAME,
        ":");
    configMap.put(TranslateDeriver.TRANSLATOR_CONFIG + "." + DelimitedTranslator.SCHEMA_CONFIG +
        "." + SchemaFactory.TYPE_CONFIG_NAME, "flat");
    configMap.put(TranslateDeriver.TRANSLATOR_CONFIG + "." + DelimitedTranslator.SCHEMA_CONFIG +
        "." + FlatSchema.FIELD_NAMES_CONFIG, Lists.newArrayList("e", "f", "g"));
    configMap.put(TranslateDeriver.TRANSLATOR_CONFIG + "." + DelimitedTranslator.SCHEMA_CONFIG +
        "." + FlatSchema.FIELD_TYPES_CONFIG, Lists.newArrayList(
            ConfigurationDataTypes.STRING, ConfigurationDataTypes.STRING, ConfigurationDataTypes.INTEGER));
    Config config = ConfigFactory.parseMap(configMap);
    TranslateDeriver deriver = new TranslateDeriver();
    ValidationAssert.assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    List<Row> derivedList = deriver.derive(dependencies).collectAsList();
    assertEquals(1, derivedList.size());
    Row derived = derivedList.get(0);
    assertArrayEquals(new String[] {"a", "b", "d", "e", "f", "g"}, derived.schema().fieldNames());
    assertArrayEquals(new Object[] {"abc", true, -1.0, "hello", "world", 1}, RowUtils.valuesFor(derived));

    List<Row> erroredList = deriver.getErroredData().collectAsList();
    assertEquals(1, erroredList.size());
    Row errored = erroredList.get(0);
    assertEquals(bad, errored);
  }

}
