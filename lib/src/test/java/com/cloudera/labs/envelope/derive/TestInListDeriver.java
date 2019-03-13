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

import com.cloudera.labs.envelope.spark.Contexts;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * In-List Deriver Unit Test
 */
public class TestInListDeriver {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testDeriveLiteral() throws Exception {
    Dataset<Row> source = createTestDataframe();

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
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
    assertThat(results.size(), is(3));

    for (Row row : results) {
      assertThat(row.getString(0), in(inListLiteral));
    }
  }


  @Test
  public void testDeriveLiteralInteger() throws Exception {
    Dataset<Row> source = createTestDataframe();

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("df1", source);
    dependencies.put("df2", null);
    dependencies.put("df3", null);

    List<Integer> inListIntegers = Arrays.asList(1, 3, 5);

    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListIntegers));

    InListDeriver deriver = new InListDeriver();
    assertNoValidationFailures(deriver, config);

    deriver.configure(config);

    List<Row> results = deriver.derive(dependencies).select("value").collectAsList();
    assertThat(results.size(), is(3));

    for (Row row : results) {
      assertThat(row.getInt(0), in(inListIntegers));
    }
  }

  @Test
  public void testDeriveLiteralIntegerCast() throws Exception {
    Dataset<Row> source = createTestDataframe();

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("df1", source);
    dependencies.put("df2", null);
    dependencies.put("df3", null);

    List<String> inListLiteral = Arrays.asList("1", "3", "5");

    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral));

    InListDeriver deriver = new InListDeriver();

    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    List<Row> results = deriver.derive(dependencies).select("value").collectAsList();
    assertThat(results.size(), is(3));

    for (Row row : results) {
      assertThat(String.valueOf(row.getInt(0)), in(inListLiteral));
    }
  }

  @Test
  public void testDeriveLiteralDate() throws Exception {
    Dataset<Row> source = createTestDataframe();

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("df1", source);
    dependencies.put("df2", null);
    dependencies.put("df3", null);

    List<String> inListLiteral = Arrays.asList("2018-04-01", "2018-04-03", "2018-04-05", "2018-04-26");

    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("vdate"))
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral));

    InListDeriver deriver = new InListDeriver();

    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    List<Row> results = deriver.derive(dependencies).select("vdate").collectAsList();
    assertThat(results.size(), is(4));

    for (Row row : results) {
      assertThat(row.getDate(0).toString(), in(inListLiteral));
    }
  }

  @Test
  public void testDeriveReference() throws Exception {
    Dataset<Row> source = createTestDataframe();
    Dataset<Row> ref = createTestDataframe().withColumnRenamed("id", "fk").filter("value < 6");

    List<String> ids = Arrays.asList("A", "B", "C", "D", "E");

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("df1", source);
    dependencies.put("df2", ref);
    dependencies.put("df3", null);

    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("id"))
        .withValue(InListDeriver.INLIST_REFSTEP_CONFIG, ConfigValueFactory.fromAnyRef("df2"))
        .withValue(InListDeriver.INLIST_REFFIELD_CONFIG, ConfigValueFactory.fromAnyRef("fk"));

    InListDeriver deriver = new InListDeriver();

    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    List<Row> results = deriver.derive(dependencies).select("id").collectAsList();
    assertThat(results.size(), is(5));

    for (Row row : results) {
      assertThat(row.getString(0), in(ids));
    }
  }

  @Test
  public void testMissingConfig() {
    Config config = ConfigFactory.empty();
    assertValidationFailures(new InListDeriver(), config);
  }

  @Test
  public void testAmbiguousConfig() {
    List<String> inListLiteral = Arrays.asList("1", "2", "3");
    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral))
        .withValue(InListDeriver.INLIST_REFSTEP_CONFIG, ConfigValueFactory.fromAnyRef("df2"));
    assertValidationFailures(new InListDeriver(), config);
  }

  @Test
  public void testMissingField() throws Exception {
    Dataset<Row> source = createTestDataframe();

    List<String> inListLiteral = Arrays.asList("1", "2", "3");

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("df1", source);

    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral));

    InListDeriver deriver = new InListDeriver();

    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    // Doesn't return anything because the IN values don't match the target column values
    List<Row> results = deriver.derive(dependencies).select("value").collectAsList();
    assertThat(results.size(), is(0));
  }

  @Test
  public void testWrongField() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Error executing IN list filtering");

    Dataset<Row> source = createTestDataframe();
    List<String> inListLiteral = Arrays.asList("1", "2", "3");

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("df1", source);

    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("non_existing_field"))
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral));

    InListDeriver deriver = new InListDeriver();

    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    deriver.derive(dependencies);
  }

  @Test
  public void testMissingRefField() throws Exception {
    Dataset<Row> source = createTestDataframe();
    Dataset<Row> ref = createTestDataframe();

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("df1", source);
    dependencies.put("df2", ref);

    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("value"))
        .withValue(InListDeriver.INLIST_REFSTEP_CONFIG, ConfigValueFactory.fromAnyRef("df2"));

    InListDeriver deriver = new InListDeriver();

    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    // Doesn't return anything because the IN values don't match the target column values
    List<Row> results = deriver.derive(dependencies).select("value").collectAsList();
    assertThat(results.size(), is(0));
  }

  @Test
  public void testWrongRefField() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Error executing IN list filtering");

    Dataset<Row> source = createTestDataframe();
    Dataset<Row> ref = createTestDataframe();

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
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

    deriver.derive(dependencies);
  }

  @Test
  public void testMissingDependencies() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("In-list deriver must specify a step if it does not only have one dependency");

    Map<String, Dataset<Row>> dependencies = new HashMap<>();

    List<String> inListLiteral = Arrays.asList("1", "2", "3");

    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral));

    InListDeriver deriver = new InListDeriver();

    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    deriver.derive(dependencies);
  }

  @Test
  public void testWrongSourceTargetDependency() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("In-list deriver does not have step 'step_df_missing' in its dependencies");

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("df1", null);
    dependencies.put("df2", null);

    List<String> inListLiteral = Arrays.asList("1", "2", "3");

    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral))
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("step_df_missing"));

    InListDeriver deriver = new InListDeriver();

    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    deriver.derive(dependencies);
  }

  @Test
  public void testWrongReferenceDependency() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Targeted step, 'df1', is null");

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("df1", null);
    dependencies.put("df2", null);

    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_REFSTEP_CONFIG, ConfigValueFactory.fromAnyRef("df_missing"))
        .withValue(InListDeriver.INLIST_REFSTEP_CONFIG, ConfigValueFactory.fromAnyRef("ref_df_missing"));

    InListDeriver deriver = new InListDeriver();

    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    deriver.derive(dependencies);
  }

  @Test
  public void testAmbiguousDependencies() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("In-list deriver must specify a step if it does not only have one dependency");

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("df1", null);
    dependencies.put("df2", null);

    List<String> inListLiteral = Arrays.asList("1", "2", "3");

    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_VALUES_CONFIG, ConfigValueFactory.fromIterable(inListLiteral));

    assertNoValidationFailures(new InListDeriver(), config);

    Deriver deriver = new InListDeriver();
    deriver.configure(config);

    deriver.derive(dependencies);
  }

  @Test
  public void testBatchSize() throws Exception {
    Dataset<Row> source = createTestDataframe();
    Dataset<Row> ref = createTestDataframe().withColumnRenamed("id", "fk").filter("value < 6");

    List<String> ids = Arrays.asList("A", "B", "C", "D", "E");

    Map<String, Dataset<Row>> dependencies = new HashMap<>();
    dependencies.put("df1", source);
    dependencies.put("df2", ref);

    Config config = ConfigFactory.empty()
        .withValue(InListDeriver.INLIST_BATCH_SIZE, ConfigValueFactory.fromAnyRef(1))
        .withValue(InListDeriver.INLIST_STEP_CONFIG, ConfigValueFactory.fromAnyRef("df1"))
        .withValue(InListDeriver.INLIST_FIELD_CONFIG, ConfigValueFactory.fromAnyRef("id"))
        .withValue(InListDeriver.INLIST_REFSTEP_CONFIG, ConfigValueFactory.fromAnyRef("df2"))
        .withValue(InListDeriver.INLIST_REFFIELD_CONFIG, ConfigValueFactory.fromAnyRef("fk"));

    InListDeriver deriver = new InListDeriver();

    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    List<Row> results = deriver.derive(dependencies).select("id").collectAsList();
    assertThat(results.size(), is(5));

    for (Row row : results) {
      assertThat(row.getString(0), in(ids));
    }
  }

  private static Dataset<Row> createTestDataframe() throws Exception {
    return Contexts.getSparkSession().createDataFrame(createTestData(), createTestSchema());
  }

  private static StructType createTestSchema() {
    return DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("descr", DataTypes.StringType, true),
        DataTypes.createStructField("value", DataTypes.IntegerType, true),
        DataTypes.createStructField("vdate", DataTypes.DateType, true))
    );
  }

  private static List<Row> createTestData() throws Exception {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    return Arrays.asList(
        RowFactory.create("A", "Alfa", 1, new Date(df.parse("2018-04-01").getTime())),
        RowFactory.create("B", "Bravo", 2, new Date(df.parse("2018-04-02").getTime())),
        RowFactory.create("C", "Charlie", 3, new Date(df.parse("2018-04-03").getTime())),
        RowFactory.create("D", "Delta", 4, new Date(df.parse("2018-04-04").getTime())),
        RowFactory.create("E", "Echo", 5, new Date(df.parse("2018-04-05").getTime())),
        RowFactory.create("F", "Foxtrot", 6, new Date(df.parse("2018-04-06").getTime())),
        RowFactory.create("G", "Golf", 7, new Date(df.parse("2018-04-07").getTime())),
        RowFactory.create("X", "XRay", 24, new Date(df.parse("2018-04-24").getTime())),
        RowFactory.create("Y", "Yankee", 25, new Date(df.parse("2018-04-25").getTime())),
        RowFactory.create("Z", "Zulu", 26, new Date(df.parse("2018-04-26").getTime())),
        RowFactory.create(null, null, null, new Date(df.parse("2018-04-30").getTime()))
    );
  }

}