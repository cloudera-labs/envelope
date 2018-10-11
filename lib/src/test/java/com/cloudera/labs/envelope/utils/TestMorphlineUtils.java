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

package com.cloudera.labs.envelope.utils;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import mockit.integration.junit4.JMockit;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers;
import org.junit.runner.RunWith;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 *
 */
@RunWith(JMockit.class)
public class TestMorphlineUtils {

  private static final String MORPHLINE_FILE = "/morphline.conf";

  private String getResourcePath(String resource) {
    return TestMorphlineUtils.class.getResource(resource).getPath();
  }

  // TODO : Pipeline tests
  @Ignore
  @Test
  public void getPipeline(
      final @Mocked ThreadLocal<Map<String, MorphlineUtils.Pipeline>> pipelineCache,
      final @Mocked Map<String, MorphlineUtils.Pipeline> cacheMap,
      final @Mocked MorphlineUtils.Pipeline returnedPipeline
  ) throws Exception {

    new Expectations() {{
      pipelineCache.get(); returns(null, cacheMap); times = 2;
      cacheMap.get("morphlineFile-morphlineId"); result = returnedPipeline; times = 1;
    }};

    assertNull("First invocation is not null", MorphlineUtils.getPipeline("morphlineFile", "morphlineId"));
  }

  @Test
  public void setPipeline() throws Exception {
    MorphlineUtils.setPipeline(getResourcePath(MORPHLINE_FILE), "default", new MorphlineUtils.Collector(), true);
  }

  @Test (expected = MorphlineCompilationException.class)
  public void setPipelineFileNotFound() throws Exception {
    MorphlineUtils.setPipeline("file", "id", new MorphlineUtils.Collector(), true);
  }

  @Test
  public void executePipeline(
      final @Mocked MorphlineUtils.Pipeline pipeline,
      final @Mocked Command morphline
  ) throws Exception {

    final Record inputRecord = new Record();

    final Record outputRecord = new Record();
    outputRecord.put("field1", "value1");

    new Expectations() {{
      morphline.process(inputRecord); result = true;
      pipeline.getCollector().getRecords(); result = Lists.newArrayList(outputRecord);
    }};

    List<Record> outputList = MorphlineUtils.executePipeline(pipeline, inputRecord);
    assertEquals(Lists.newArrayList(outputRecord), outputList);
  }

  @Test (expected = MorphlineRuntimeException.class)
  public void executePipelineProcessError(
      final @Mocked MorphlineUtils.Pipeline pipeline,
      final @Mocked Command morphline
  ) throws Exception {

    final Record inputRecord = new Record();

    new Expectations() {{
      morphline.process(inputRecord); result = false;
    }};

    MorphlineUtils.executePipeline(pipeline, inputRecord);
  }

  @Test (expected = MorphlineRuntimeException.class)
  public void executePipelineNoRecords(
      final @Mocked MorphlineUtils.Pipeline pipeline,
      final @Mocked Command morphline
  ) throws Exception {

    final Record inputRecord = new Record();

    new Expectations() {{
      morphline.process(inputRecord); result = true;
      pipeline.getCollector().getRecords(); result = Lists.newArrayList();
    }};

    MorphlineUtils.executePipeline(pipeline, inputRecord);
  }

  @Test
  public void executePipelineNoRecordsNoError(
      final @Mocked MorphlineUtils.Pipeline pipeline,
      final @Mocked Command morphline
  ) throws Exception {

    final Record inputRecord = new Record();

    new Expectations() {{
      morphline.process(inputRecord); result = true;
      pipeline.getCollector().getRecords(); result = Lists.newArrayList();
    }};

    assertEquals("Invalid number of Rows returned", 0, MorphlineUtils.executePipeline(pipeline, inputRecord, false).size());
  }

  @Test
  public void morphlineMapper(
      final @Mocked MorphlineUtils.Pipeline pipeline,
      final @Mocked Row row,
      final @Mocked StructType schema
  ) throws Exception {

    new Expectations(MorphlineUtils.class) {{
      MorphlineUtils.getPipeline("file", "id"); result = pipeline; times = 1;
      MorphlineUtils.executePipeline(pipeline, (Record) any, true); result = Lists.newArrayList(); times = 1;
      row.schema(); result = schema;
      row.get(anyInt); returns("val1", "val2"); times = 2;
      schema.fieldNames(); result = new String[] { "one", "two"};
    }};

    FlatMapFunction<Row, Row> function = MorphlineUtils.morphlineMapper("file", "id", schema, true);
    Iterator<Row> results = function.call(row);

    assertEquals("Invalid number of Rows returned", 0, Lists.newArrayList(results).size());

    new Verifications() {{
      Record record;
      MorphlineUtils.executePipeline(pipeline, record = withCapture(), true);
      assertEquals(2, record.getFields().size());
      assertEquals("val1", record.get("one").get(0));
    }};
  }

  @Test
  public void morphlineMapperNoPipeline(
      final @Mocked MorphlineUtils.Pipeline pipeline,
      final @Mocked Row row,
      final @Mocked StructType schema
  ) throws Exception {

    new Expectations(MorphlineUtils.class) {{
      MorphlineUtils.getPipeline("file", "id"); result = null; times = 1;
      MorphlineUtils.setPipeline("file", "id", (MorphlineUtils.Collector) any, true); result = pipeline; times = 1;
      MorphlineUtils.executePipeline(pipeline, (Record) any, true); result = Lists.newArrayList(); times = 1;
      row.schema(); result = schema;
      row.get(anyInt); returns("val1", "val2"); times = 2;
      schema.fieldNames(); result = new String[] { "one", "two"};
    }};

    FlatMapFunction<Row, Row> function = MorphlineUtils.morphlineMapper("file", "id", schema, true);
    Iterator<Row> results = function.call(row);

    assertEquals("Invalid number of Rows returned", 0, Lists.newArrayList(results).size());

    new Verifications() {{
      Record record;
      MorphlineUtils.executePipeline(pipeline, record = withCapture(), true);
      assertEquals(2, record.getFields().size());
      assertEquals("val1", record.get("one").get(0));
    }};
  }

  @Test (expected = RuntimeException.class)
  public void morphlineMapperNoSchema(
      final @Mocked MorphlineUtils.Pipeline pipeline,
      final @Mocked Row row,
      final @Mocked StructType schema
  ) throws Exception {

    new Expectations(MorphlineUtils.class) {{
      MorphlineUtils.getPipeline("file", "id"); result = pipeline; times = 1;
      row.schema(); result = null;
    }};

    FlatMapFunction<Row, Row> function = MorphlineUtils.morphlineMapper("file", "id", schema, true);
    function.call(row);
  }

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
      assertThat(e.getMessage(), JUnitMatchers.containsString("DataType cannot contain 'null'"));
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

    MorphlineUtils.convertToRow(schema, record);

    new Verifications() {{
      RowUtils.toRowValue(any, (DataType) any); times = 0;
    }};
  }
}