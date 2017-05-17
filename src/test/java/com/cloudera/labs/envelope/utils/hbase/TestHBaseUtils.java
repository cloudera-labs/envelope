/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.utils.hbase;

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestHBaseUtils {

  @Test
  public void testValidateConfig() {
    Config goodConfig = ConfigUtils.configFromResource("/hbase/hbase-output.conf").getConfig("output");
    assertTrue("Good config should validate", HBaseUtils.validateConfig(goodConfig));

    Config badConfigNoCols = ConfigUtils.configFromResource("/hbase/hbase-output-no-columns.conf").getConfig("output");
    assertFalse("Config with no columns should not validate",
        HBaseUtils.validateConfig(badConfigNoCols));

    Config badConfigNoRK = ConfigUtils.configFromResource("/hbase/hbase-output-no-rowkey.conf").getConfig("output");
    assertFalse("Config with no rowkey should not validate",
        HBaseUtils.validateConfig(badConfigNoRK));

    Config badConfigBadCols = ConfigUtils.configFromResource("/hbase/hbase-output-badcol.conf").getConfig("output");
    assertFalse("Config with bad columns should not validate",
        HBaseUtils.validateConfig(badConfigBadCols));
  }

  @Test
  public void testPassthroughHBaseOptions() throws IOException {
    Config goodConfig = ConfigUtils.configFromResource("/hbase/hbase-output-with-hbase-config.conf").getConfig("output");
    assertTrue("Good config with HBase options should validate", HBaseUtils.validateConfig(goodConfig));

    Configuration hBaseConfiguration = HBaseUtils.getHBaseConfiguration(goodConfig);

    assertEquals("HBase configuration should have custom retries", 5, Integer.parseInt(hBaseConfiguration.get("hbase.client.retries.number")));
    assertEquals("HBase configuration should have custom timeout", 30000, Integer.parseInt(hBaseConfiguration.get("hbase.client.operation.timeout")));
  }

  @Test
  public void testRowKeyFor() {
    Config goodConfig = ConfigUtils.configFromResource("/hbase/hbase-output.conf").getConfig("output");

    List<String> rowKeys = HBaseUtils.rowKeyFor(goodConfig);
    assertEquals(2, rowKeys.size());
    assertEquals("symbol", rowKeys.get(0));
    assertEquals("transacttime", rowKeys.get(1));
  }

  @Test
  public void testRowKeySeparatorFor() {
    Config goodConfig = ConfigUtils.configFromResource("/hbase/hbase-output.conf").getConfig("output");

    byte[] rowKeySeparatorFor = HBaseUtils.rowKeySeparatorFor(goodConfig);
    assertArrayEquals(":".getBytes(), rowKeySeparatorFor);
  }

  @Test
  public void testCustomRowKeySeparatorFor() {
    Config goodConfig = ConfigUtils.configFromResource("/hbase/hbase-output-with-different-keysep.conf").getConfig("output");

    byte[] rowKeySeparatorFor = HBaseUtils.rowKeySeparatorFor(goodConfig);
    assertArrayEquals("^^".getBytes(), rowKeySeparatorFor);
  }

  @Test
  public void testColumnsFor() {
    Config goodConfig = ConfigUtils.configFromResource("/hbase/hbase-output.conf").getConfig("output");

    Map<String, HBaseSerde.ColumnDef> columnDefs = HBaseUtils.columnsFor(goodConfig);
    assertEquals(6, columnDefs.size());
    HBaseSerde.ColumnDef def = columnDefs.get("symbol");
    assertEquals("rowkey", def.cf);
    assertEquals("symbol", def.name);
    assertEquals("string", def.type);
    def = columnDefs.get("transacttime");
    assertEquals("rowkey", def.cf);
    assertEquals("transacttime", def.name);
    assertEquals("long", def.type);
    def = columnDefs.get("clordid");
    assertEquals("cf1", def.cf);
    assertEquals("clordid", def.name);
    assertEquals("string", def.type);
    def = columnDefs.get("orderqty");
    assertEquals("cf1", def.cf);
    assertEquals("orderqty", def.name);
    assertEquals("int", def.type);
    def = columnDefs.get("leavesqty");
    assertEquals("cf1", def.cf);
    assertEquals("leavesqty", def.name);
    assertEquals("int", def.type);
    def = columnDefs.get("cumqty");
    assertEquals("cf1", def.cf);
    assertEquals("cumqty", def.name);
    assertEquals("int", def.type);
  }

  @Test
  public void testBuildSchema() {
    Config goodConfig = ConfigUtils.configFromResource("/hbase/hbase-output.conf").getConfig("output");

    Map<String, HBaseSerde.ColumnDef> columnDefs = HBaseUtils.columnsFor(goodConfig);
    StructType schema = HBaseUtils.buildSchema(columnDefs);

    Map<String, DataType> shouldHaveFields = Maps.newHashMap();
    shouldHaveFields.put("symbol", DataTypes.StringType);
    shouldHaveFields.put("transacttime", DataTypes.LongType);
    shouldHaveFields.put("clordid", DataTypes.StringType);
    shouldHaveFields.put("orderqty", DataTypes.IntegerType);
    shouldHaveFields.put("leavesqty", DataTypes.IntegerType);
    shouldHaveFields.put("cumqty", DataTypes.IntegerType);

    StructField[] fields = schema.fields();
    assertEquals("Schema should contain 6 fields", 6, fields.length);

    Map<String, DataType> hasFields = Maps.newHashMap();
    for (StructField field : fields) {
      hasFields.put(field.name(), field.dataType());
    }

    for (Map.Entry<String, DataType> shouldHave : shouldHaveFields.entrySet()) {
      assertTrue("Schema should have field: " + shouldHave.getKey(),
          hasFields.containsKey(shouldHave.getKey()));
      assertEquals("Field " + shouldHave.getKey() + " should have type: " +
              shouldHave.getValue(), shouldHave.getValue(),
          hasFields.get(shouldHave.getKey()));
    }

  }

  @Test
  public void testTableInfoFor() {
    Config goodConfig = ConfigUtils.configFromResource("/hbase/hbase-output.conf").getConfig("output");

    TableName tableName = HBaseUtils.tableInfoFor(goodConfig);
    assertArrayEquals("default".getBytes(), tableName.getNamespace());
    assertArrayEquals("test".getBytes(), tableName.getName());
  }

  @Test
  public void testBatchSizeFor() {
    Config goodConfig = ConfigUtils.configFromResource("/hbase/hbase-output.conf").getConfig("output");

    int batchSize = HBaseUtils.batchSizeFor(goodConfig);
    assertEquals(HBaseUtils.DEFAULT_HBASE_BATCH_SIZE, batchSize);

    Config goodConfigWithBatchSize = ConfigUtils.configFromResource("/hbase/hbase-output-with-batchsize.conf").getConfig("output");

    batchSize = HBaseUtils.batchSizeFor(goodConfigWithBatchSize);
    assertEquals(100, batchSize);
  }

}
