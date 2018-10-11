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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.input.translate.TestMorphlineTranslator;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.MorphlineUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kitesdk.morphline.api.MorphlineCompilationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
@RunWith(JMockit.class)
public class TestMorphlineDeriver {

  private static final String MORPHLINE_FILE = "/morphline.conf";

  private String getResourcePath(String resource) {
    return TestMorphlineTranslator.class.getResource(resource).getPath();
  }

  @Test
  public void getSchema() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(MorphlineDeriver.STEP_NAME_CONFIG, "dep1");
    paramMap.put(MorphlineDeriver.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    paramMap.put(MorphlineDeriver.MORPHLINE_ID, "id");
    paramMap.put(MorphlineDeriver.FIELD_NAMES, Lists.newArrayList("foo", "bar"));
    paramMap.put(MorphlineDeriver.FIELD_TYPES, Lists.newArrayList("int", "string"));
    final Config config = ConfigFactory.parseMap(paramMap);

    MorphlineDeriver deriver = new MorphlineDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    StructType schema = deriver.getSchema();

    assertEquals("Invalid number of SchemaFields", 2, schema.fields().length);
    assertEquals("Invalid DataType", DataTypes.IntegerType, schema.fields()[0].dataType());
    assertEquals("Invalid DataType", DataTypes.StringType, schema.fields()[1].dataType());
  }

  @Test (expected = RuntimeException.class)
  public void getSchemaInvalidDataType() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(MorphlineDeriver.STEP_NAME_CONFIG, "dep1");
    paramMap.put(MorphlineDeriver.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    paramMap.put(MorphlineDeriver.MORPHLINE_ID, "id");
    paramMap.put(MorphlineDeriver.FIELD_NAMES, Lists.newArrayList("bar"));
    paramMap.put(MorphlineDeriver.FIELD_TYPES, Lists.newArrayList("boom"));
    final Config config = ConfigFactory.parseMap(paramMap);

    MorphlineDeriver deriver = new MorphlineDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
  }

  @Test
  public void deriveNullMorphlineConfig() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(MorphlineDeriver.STEP_NAME_CONFIG, "dep1");
    paramMap.put(MorphlineDeriver.MORPHLINE, null);
    final Config config = ConfigFactory.parseMap(paramMap);

    MorphlineDeriver deriver = new MorphlineDeriver();
    assertValidationFailures(deriver, config);
  }

  @Test
  public void deriveBlankMorphlineConfig() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(MorphlineDeriver.STEP_NAME_CONFIG, "dep1");
    paramMap.put(MorphlineDeriver.MORPHLINE, "");
    final Config config = ConfigFactory.parseMap(paramMap);

    MorphlineDeriver deriver = new MorphlineDeriver();
    assertValidationFailures(deriver, config);
  }

  @Test
  public void deriveMissingStepName() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(MorphlineDeriver.STEP_NAME_CONFIG, null);
    final Config config = ConfigFactory.parseMap(paramMap);

    MorphlineDeriver deriver = new MorphlineDeriver();
    assertValidationFailures(deriver, config);
  }

  @Test
  public void deriveBlankStepName() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(MorphlineDeriver.STEP_NAME_CONFIG, "");
    final Config config = ConfigFactory.parseMap(paramMap);

    MorphlineDeriver deriver = new MorphlineDeriver();
    assertValidationFailures(deriver, config);
  }

  @Test (expected = RuntimeException.class)
  public void deriveMissingStepDependency() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(MorphlineDeriver.STEP_NAME_CONFIG, "nope");
    paramMap.put(MorphlineDeriver.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    paramMap.put(MorphlineDeriver.MORPHLINE_ID, "id");
    paramMap.put(MorphlineDeriver.FIELD_NAMES, Lists.newArrayList("bar"));
    paramMap.put(MorphlineDeriver.FIELD_TYPES, Lists.newArrayList("int"));
    final Config config = ConfigFactory.parseMap(paramMap);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dep1", null);
    dependencies.put("dep2", null);

    MorphlineDeriver deriver = new MorphlineDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    deriver.derive(dependencies);
  }

  @Test (expected = RuntimeException.class)
  public void deriveMorphlineMapperFunctionError(
      final @Mocked MorphlineUtils utils
  ) throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(MorphlineDeriver.STEP_NAME_CONFIG, "dep1");
    paramMap.put(MorphlineDeriver.MORPHLINE, "morphline");
    paramMap.put(MorphlineDeriver.MORPHLINE_ID, "id");
    paramMap.put(MorphlineDeriver.FIELD_NAMES, Lists.newArrayList("bar"));
    paramMap.put(MorphlineDeriver.FIELD_TYPES, Lists.newArrayList("int"));
    final Config config = ConfigFactory.parseMap(paramMap);

    new Expectations() {{
      MorphlineUtils.morphlineMapper(anyString, anyString, (StructType) any, true); result =
          new MorphlineCompilationException("Compile exception", config);
    }};

    Dataset<Row> dataFrame = Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(RowFactory.create(1)),
        DataTypes.createStructType(Lists.newArrayList(DataTypes.createStructField("baz", DataTypes.IntegerType, false)))
    );

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dep1", dataFrame);

    MorphlineDeriver deriver = new MorphlineDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    deriver.derive(dependencies);
  }

  @Test
  public void deriveIntegrationTest() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(MorphlineDeriver.STEP_NAME_CONFIG, "dep1");
    paramMap.put(MorphlineDeriver.MORPHLINE, getResourcePath(MORPHLINE_FILE));
    paramMap.put(MorphlineDeriver.MORPHLINE_ID, "deriver");
    paramMap.put(MorphlineDeriver.FIELD_NAMES, Lists.newArrayList("foo", "bar", "baz"));
    paramMap.put(MorphlineDeriver.FIELD_TYPES, Lists.newArrayList("string", "int", "int"));
    final Config config = ConfigFactory.parseMap(paramMap);

    Dataset<Row> dataFrame = Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(RowFactory.create(987, "string value")),
        DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("one", DataTypes.IntegerType, false),
            DataTypes.createStructField("two", DataTypes.StringType, false))
        )
    );

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dep1", dataFrame);

    MorphlineDeriver deriver = new MorphlineDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    Dataset<Row> outputDF = deriver.derive(dependencies);
    outputDF.printSchema();
    List<Row> rowList = outputDF.collectAsList();
    assertEquals(1, rowList.size());
    assertEquals(3, rowList.get(0).size());
    assertTrue(rowList.get(0).get(0) instanceof String);
    assertTrue(rowList.get(0).get(1) instanceof Integer);
    assertTrue(rowList.get(0).get(2) instanceof Integer);
  }

}