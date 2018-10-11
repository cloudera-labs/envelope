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

import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * In-List Deriver Unit Test
 */
public class TestInListDeriver {

  @Test
  public void testDerive() throws Exception {
    Dataset<Row> source = createTestDataframe();
    source.show();
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("df1", source);
    dependencies.put("df2", null);
    dependencies.put("df3", null);
    List<String> inListLiteral = Arrays.asList("A", "C", "E");
    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("id"))
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral));
    InListDeriver deriver = new InListDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    List<Row> results = deriver.derive(dependencies).select("id").collectAsList();
    assertEquals(deriver.getInList(dependencies).length, results.size());
    for (Row row : results) {
      Object x = row.get(0);
      assertTrue(inListLiteral.contains(x.toString()));
    }

    inListLiteral = Arrays.asList("1", "3", "5");
    config = config.withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral));
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    results = deriver.derive(dependencies).select("value").collectAsList();
    assertEquals(deriver.getInList(dependencies).length, results.size());
    for (Row row : results) {
      Object x = row.get(0);
      assertTrue(inListLiteral.contains(x.toString()));
    }

    List<Integer> inListIntegers = Arrays
        .asList(Integer.valueOf("1"), Integer.valueOf("3"), Integer.valueOf("5"));
    config = config.withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListIntegers));
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    results = deriver.derive(dependencies).select("value").collectAsList();
    assertEquals(deriver.getInList(dependencies).length, results.size());
    for (Row row : results) {
      Object x = row.get(0);
      assertTrue(inListIntegers.contains(x));
    }

    inListLiteral = Arrays.asList("2018-04-01", "2018-04-03", "2018-04-05", "2018-04-26");
    config = config.withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("vdate"))
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral));
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    results = deriver.derive(dependencies).select("vdate").collectAsList();
    assertEquals(deriver.getInList(dependencies).length, results.size());
    for (Row row : results) {
      Object x = row.get(0);
      assertTrue(inListLiteral.contains(x.toString()));
    }

    Dataset<Row> ref = createTestDataframe();
    ref = ref.withColumnRenamed("id", "fk").filter("value < 6");
    dependencies.put("df2", ref);
    config = config.withoutPath(InListDeriver.INLIST_VALUES_CONFIG)
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("id"))
        .withValue(InListDeriver.INLIST_REFSTEP_CONFIG, ConfigValueFactory.fromAnyRef("df2"))
        .withValue(InListDeriver.INLIST_REFFIELD_CONFIG, ConfigValueFactory.fromAnyRef("fk"));
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    results = deriver.derive(dependencies).select("id").collectAsList();
    assertEquals(ref.count(), results.size());
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void missingConfig() throws Exception {
    Config config = ConfigFactory.empty();
    assertValidationFailures(new InListDeriver(), config);
  }

  @Test
  public void ambiguousConfig() throws Exception {
    List<String> inListLiteral = Arrays.asList("1", "2", "3");
    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral))
        .withValue(InListDeriver.INLIST_REFSTEP_CONFIG, ConfigValueFactory.fromAnyRef("df2"));
    assertValidationFailures(new InListDeriver(), config);
  }

  @Test
  public void missingField() throws Exception {
    Dataset<Row> source = createTestDataframe();
    List<String> inListLiteral = Arrays.asList("1", "2", "3");
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("df1", source);
    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral))
        .withoutPath(InListDeriver.INLIST_FIELD_CONFIG);
    InListDeriver deriver = new InListDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("parameter should be specified");
    deriver.derive(dependencies);
  }

  @Test
  public void wrongField() throws Exception {
    Dataset<Row> source = createTestDataframe();
    List<String> inListLiteral = Arrays.asList("1", "2", "3");
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("df1", source);
    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("non_existing_field"))
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral));
    InListDeriver deriver = new InListDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("doesn't contain \"" + InListDeriver.INLIST_FIELD_CONFIG + "\"");
    deriver.derive(dependencies);
  }

  @Test
  public void missingRefField() throws Exception {
    Dataset<Row> source = createTestDataframe();
    Dataset<Row> ref = createTestDataframe();
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("df1", source);
    dependencies.put("df2", ref);
    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(InListDeriver.INLIST_REFSTEP_CONFIG, ConfigValueFactory.fromAnyRef("df2"))
        .withoutPath(InListDeriver.INLIST_REFFIELD_CONFIG);
    InListDeriver deriver = new InListDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("\"" + InListDeriver.INLIST_REFFIELD_CONFIG + "\" parameter should be specified");
    deriver.derive(dependencies);
  }

  @Test
  public void wrongRefField() throws Exception {
    Dataset<Row> source = createTestDataframe();
    Dataset<Row> ref = createTestDataframe();
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("df1", source);
    dependencies.put("df2", ref);
    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(InListDeriver.INLIST_REFSTEP_CONFIG, ConfigValueFactory.fromAnyRef("df2"))
        .withValue(InListDeriver.INLIST_REFFIELD_CONFIG, ConfigValueFactory.fromAnyRef("non_existent_ref_field"));
    InListDeriver deriver = new InListDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("doesn't contain \"" + InListDeriver.INLIST_REFFIELD_CONFIG + "\"");
    deriver.derive(dependencies);
  }

  @Test
  public void missingDependencies() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("at least one dependency");
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    List<String> inListLiteral = Arrays.asList("1", "2", "3");
    Config config = ConfigFactory.empty().withValue(InListDeriver.INLIST_VALUES_CONFIG,
        ConfigValueFactory.fromIterable(inListLiteral));
    InListDeriver deriver = new InListDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    deriver.derive(dependencies);
  }

  @Test
  public void wrongDependencies() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("not listed as dependency");
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    List<String> inListLiteral = Arrays.asList("1", "2", "3");
    dependencies.put("df1", null);
    dependencies.put("df2", null);
    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral))
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df_missing"));
    InListDeriver deriver = new InListDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    deriver.derive(dependencies);

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("not listed as dependency");
    config.withoutPath(InListDeriver.INLIST_VALUES_CONFIG)
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_REFSTEP_CONFIG, ConfigValueFactory.fromAnyRef("df_missing"));
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    deriver.derive(dependencies);
  }

  @Test
  public void ambiguousDependencies() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("multiple dependencies have been listed");
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    List<String> inListLiteral = Arrays.asList("1", "2", "3");
    dependencies.put("df1", null);
    dependencies.put("df2", null);
    Config config = ConfigFactory.empty().withValue(InListDeriver.INLIST_VALUES_CONFIG,
        ConfigValueFactory.fromIterable(inListLiteral));
    InListDeriver deriver = new InListDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);
    deriver.derive(dependencies);
  }

  @Test
  public void overflowList() throws Exception {
    Dataset<Row> source = createTestDataframe();
    StructType refSchema = DataTypes
        .createStructType(Lists.newArrayList(DataTypes.createStructField("fk", DataTypes.IntegerType, true)));
    List<Row> refRows = Lists.newArrayList();
    for (int i = 0; i < InListDeriver.INLIST_MAX_LIST_SIZE + 100; i++) {
      refRows.add(RowFactory.create(i + 1));
    }
    Dataset<Row> ref = Contexts.getSparkSession().createDataFrame(refRows, refSchema);

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("df1", source);
    dependencies.put("df2", ref);
    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(InListDeriver.INLIST_REFSTEP_CONFIG, ConfigValueFactory.fromAnyRef("df2"))
        .withValue(InListDeriver.INLIST_REFFIELD_CONFIG, ConfigValueFactory.fromAnyRef("fk"));
    InListDeriver deriver = new InListDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Maximum allowed size");
    deriver.derive(dependencies);
  }

  private static Dataset<Row> createTestDataframe() throws Exception {
    StructType schema = createTestSchema();
    List<Row> rows = createTestData();
    return Contexts.getSparkSession().createDataFrame(rows, schema);
  }

  private static StructType createTestSchema() throws Exception {
    return DataTypes.createStructType(Lists.newArrayList(DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("descr", DataTypes.StringType, true),
        DataTypes.createStructField("value", DataTypes.IntegerType, true),
        DataTypes.createStructField("vdate", DataTypes.DateType, true)));
  }

  private static List<Row> createTestData() throws Exception {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    return Lists.newArrayList(RowFactory.create("A", "Alfa", 1, new Date(df.parse("2018-04-01").getTime())),
        RowFactory.create("B", "Bravo", 2, new Date(df.parse("2018-04-02").getTime())),
        RowFactory.create("C", "Charlie", 3, new Date(df.parse("2018-04-03").getTime())),
        RowFactory.create("D", "Delta", 4, new Date(df.parse("2018-04-04").getTime())),
        RowFactory.create("E", "Echo", 5, new Date(df.parse("2018-04-05").getTime())),
        RowFactory.create("F", "Foxtrot", 6, new Date(df.parse("2018-04-06").getTime())),
        RowFactory.create("G", "Golf", 7, new Date(df.parse("2018-04-07").getTime())),
        RowFactory.create("X", "XRay", 24, new Date(df.parse("2018-04-24").getTime())),
        RowFactory.create("Y", "Yankee", 25, new Date(df.parse("2018-04-25").getTime())),
        RowFactory.create("Z", "Zulu", 26, new Date(df.parse("2018-04-26").getTime())),
        RowFactory.create(null, null, null, new Date(df.parse("2018-04-30").getTime())));
  }

}