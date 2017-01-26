package com.cloudera.labs.envelope.utils;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import mockit.integration.junit4.JMockit;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers;
import org.junit.runner.RunWith;
import org.kitesdk.morphline.api.Record;

/**
 *
 */
@RunWith(JMockit.class)
public class MorphlineUtilsTest {

  @Test
  public void convertToRowValidValue(
      final @Mocked RowUtils utils
  ) throws Exception {

    Record record = new Record();
    record.put("field1", "one");

    StructType schema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.StringType, false))
    );

    new Expectations() {{
      RowUtils.toRowValue("one", DataTypes.StringType); result = "success";
    }};

    assertEquals("Invalid conversion", "success", MorphlineUtils.convertToRow(schema, record).get(0));
  }

  @Test
  public void convertToRowValidNullValue(
      final @Mocked RowUtils utils
  ) throws Exception {

    Record record = new Record();
    record.put("field1", null);

    StructType schema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.StringType, true))
    );

    assertEquals("Invalid conversion", null, MorphlineUtils.convertToRow(schema, record).get(0));

    new Verifications() {{
      RowUtils.toRowValue(any, (DataType) any); times = 0;
    }};
  }

  @Test
  public void convertToRowInvalidNullValue(
      final @Mocked RowUtils utils
  ) throws Exception {

    Record record = new Record();
    record.put("field1", null);

    StructType schema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.StringType, false))
    );

    try {
      MorphlineUtils.convertToRow(schema, record);
      fail("Did not throw a RuntimeException");
    } catch (Exception e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("DataType cannot contain 'null'"));
    }

    new Verifications() {{
      RowUtils.toRowValue(any, (DataType) any); times = 0;
    }};
  }

  @Test
  public void convertToRowInvalidTypeNotNullable(
      final @Mocked RowUtils utils
  ) throws Exception {

    Record record = new Record();
    record.put("field1", "one");

    StructType schema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.StringType, false))
    );

    new Expectations() {{
      RowUtils.toRowValue("one", DataTypes.StringType); result = new RuntimeException("Conversion exception");
    }};

    try {
      MorphlineUtils.convertToRow(schema, record);
      fail("Did not throw a RuntimeException");
    } catch (Exception e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Error converting Field"));
    }
  }

  @Test
  public void convertToRowInvalidTypeNullable(
      final @Mocked RowUtils utils
  ) throws Exception {

    Record record = new Record();
    record.put("field1", "one");

    StructType schema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.StringType, true))
    );

    new Expectations() {{
      RowUtils.toRowValue("one", DataTypes.StringType); result = new RuntimeException("Conversion exception");
    }};

    try {
      MorphlineUtils.convertToRow(schema, record);
      fail("Did not throw a RuntimeException");
    } catch (Exception e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Error converting Field"));
    }
  }

  @Test
  public void convertToRowMissingColumnNotNullable(
      final @Mocked RowUtils utils
  ) throws Exception {

    Record record = new Record();
    record.put("foo", "one");

    StructType schema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.StringType, false))
    );

    try {
      MorphlineUtils.convertToRow(schema, record);
      fail("Did not throw a RuntimeException");
    } catch (Exception e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Error converting Record"));
    }

    new Verifications() {{
      RowUtils.toRowValue(any, (DataType) any); times = 0;
    }};
  }

  @Test
  public void convertToRowMissingColumnNullable(
      final @Mocked RowUtils utils
  ) throws Exception {

    Record record = new Record();
    record.put("foo", "one");

    StructType schema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("field1", DataTypes.StringType, true))
    );

    try {
      MorphlineUtils.convertToRow(schema, record);
      fail("Did not throw a RuntimeException");
    } catch (Exception e) {
      assertThat(e.getMessage(), JUnitMatchers.containsString("Error converting Record"));
    }

    new Verifications() {{
      RowUtils.toRowValue(any, (DataType) any); times = 0;
    }};
  }
}