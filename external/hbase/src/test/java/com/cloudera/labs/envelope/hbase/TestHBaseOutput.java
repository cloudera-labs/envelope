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

package com.cloudera.labs.envelope.hbase;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.PlannerUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestHBaseOutput {

  private static final byte[] TABLE = "test".getBytes();
  private static final byte[] CF1 = "cf1".getBytes();

  private static final int INPUT_ROWS = 100;
  private static String[] SYMBOLS = {
      "AAPL", "MSFT", "ORCL", "VMW", "GOOG", "AMZN", "FB", "TWTR"
  };

  private static HBaseTestingUtility utility;
  private static Connection connection;
  private static Config appConfig = ConfigUtils.configFromResource("/hbase/hbase-output-test.conf");

  private StructType planningSchema = new StructType(
      new StructField[]{
          new StructField("symbol", DataTypes.StringType, false, Metadata.empty()),
          new StructField("transacttime", DataTypes.LongType, false, Metadata.empty()),
          new StructField("clordid", DataTypes.StringType, true, Metadata.empty()),
          new StructField("orderqty", DataTypes.IntegerType, true, Metadata.empty()),
          new StructField("leavesqty", DataTypes.IntegerType, true, Metadata.empty()),
          new StructField("cumqty", DataTypes.IntegerType, true, Metadata.empty())
      }
  );

  private StructType filterSchema = new StructType(
      new StructField[]{
          new StructField("symbol", DataTypes.StringType, false, Metadata.empty()),
          new StructField("transacttime", DataTypes.LongType, false, Metadata.empty())
      }
  );

  private void addEntriesToHBase() throws IOException {
    long beginTime = 1_000_000_000;
    Table table = connection.getTable(TableName.valueOf(TABLE));
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < INPUT_ROWS; ++i) {
      // Nonsense values
      byte[] rowKey = Bytes.add((SYMBOLS[i % SYMBOLS.length] + ":").getBytes(),
          Bytes.toBytes(beginTime + i));
      Put put = new Put(rowKey);
      put.addColumn(CF1, "clordid".getBytes(), Bytes.toBytes(Integer.toString(i)));
      put.addColumn(CF1, "orderqty".getBytes(), Bytes.toBytes(i));
      put.addColumn(CF1, "leavesqty".getBytes(), Bytes.toBytes(i % 10));
      put.addColumn(CF1, "cumqty".getBytes(), Bytes.toBytes(i + i % 10));
      puts.add(put);
    }
    table.put(puts);
    table.close();
  }

  private List<Row> createPlannedMutations() {
    List<Row> records = new ArrayList<>();
    long beginTime = 1_000_000_000;
    for (int i = 0; i < INPUT_ROWS; ++i) {
      // Nonsense
      Row insertRow = new RowWithSchema(
          planningSchema,
          SYMBOLS[i % SYMBOLS.length],
          beginTime + i,
          Integer.toString(i),
          i,
          i % 10,
          i + i % 10
      );
      records.add(PlannerUtils.setMutationType(insertRow, MutationType.UPSERT));
      Row updateRow = RowUtils.set(insertRow, "leavesqty", (i % 10) + 10);
      records.add(PlannerUtils.setMutationType(updateRow, MutationType.UPSERT));
    }

    return records;
  }

  private Dataset<Row> createBulkMutations(int num) {
    List<Row> records = new ArrayList<>();
    long beginTime = 1_000_000_000;
    for (int i = 0; i < num; ++i) {
      // Nonsense
      Row insertRow = new RowWithSchema(
          planningSchema,
          SYMBOLS[i % SYMBOLS.length],
          beginTime + i,
          Integer.toString(i),
          i,
          i % 10,
          i + i % 10
      );
      records.add(insertRow);
    }

    return Contexts.getSparkSession().createDataFrame(records, planningSchema);
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    utility = new HBaseTestingUtility();
    utility.startMiniZKCluster();
    utility.startMiniHBaseCluster(1,1);
    connection = utility.getConnection();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    Contexts.closeSparkSession(true);
    utility.shutdownMiniHBaseCluster();
    utility.shutdownMiniZKCluster();
  }

  @Before
  public void before() throws Exception {
    utility.createTable(TABLE, new byte[][]{CF1});
  }

  @After
  public void after() throws Exception {
    utility.deleteTable(TABLE);
  }


  @Test
  public void testGetExistingForFilters() throws Exception {
    addEntriesToHBase();
    Table table = connection.getTable(TableName.valueOf(TABLE));
    scanAndCountTable(table, INPUT_ROWS * 4);

    Config config = ConfigUtils.configFromResource("/hbase/hbase-output-test.conf").getConfig("output");
    config = config.withValue("zookeeper",
        ConfigValueFactory.fromAnyRef("localhost:" + utility.getZkCluster().getClientPort()));

    HBaseOutput output = new HBaseOutput();
    output.configure(config);
    List<Row> filters = Lists.newArrayList();
    filters.add(new RowWithSchema(filterSchema, "AAPL", 1_000_000_000L));
    filters.add(new RowWithSchema(filterSchema, "GOOG", 1_000_000_004L));

    Iterable<Row> filtered = output.getExistingForFilters(filters);
    assertEquals(2, Iterables.size(filtered));

    Row row1 = Iterables.get(filtered, 0);
    Row row2 = Iterables.get(filtered, 1);
    if (!row1.get(row1.fieldIndex("symbol")).equals("GOOG")) {
      Row tmp = row1;
      row1 = row2;
      row2 = tmp;
    }
    assertEquals(6, row1.schema().length());
    assertEquals("GOOG", row1.get(row1.fieldIndex("symbol")));
    assertEquals(1_000_000_004L, (long) row1.get(row1.fieldIndex("transacttime")));
    assertEquals("4", row1.get(row1.fieldIndex("clordid")));
    assertEquals(4, (int) row1.get(row1.fieldIndex("orderqty")));
    assertEquals(4, (int) row1.get(row1.fieldIndex("leavesqty")));
    assertEquals(8, (int) row1.get(row1.fieldIndex("cumqty")));
    assertEquals(6, row2.schema().length());
    assertEquals("AAPL", row2.get(row2.fieldIndex("symbol")));
    assertEquals(1_000_000_000L, row2.get(row2.fieldIndex("transacttime")));
    assertEquals("0", row2.get(row2.fieldIndex("clordid")));
    assertEquals(0, (int) row2.get(row2.fieldIndex("orderqty")));
    assertEquals(0, (int) row2.get(row2.fieldIndex("leavesqty")));
    assertEquals(0, (int) row2.get(row2.fieldIndex("cumqty")));
  }
  
  @Test
  public void testGetPartialKey() throws Exception {
    addEntriesToHBase();
    Table table = connection.getTable(TableName.valueOf(TABLE));
    scanAndCountTable(table, INPUT_ROWS * 4);

    Config config = ConfigUtils.configFromResource("/hbase/hbase-output-test.conf").getConfig("output");
    config = config.withValue("zookeeper",
        ConfigValueFactory.fromAnyRef("localhost:" + utility.getZkCluster().getClientPort()));

    HBaseOutput output = new HBaseOutput();
    output.configure(config);

    StructType partialKeySchema = new StructType(new StructField[] {
        new StructField("symbol", DataTypes.StringType, false, null)
    });
    List<Row> filters = Lists.newArrayList();
    filters.add(new RowWithSchema(partialKeySchema, "AAPL"));
    filters.add(new RowWithSchema(partialKeySchema, "GOOG"));

    Iterable<Row> filtered = output.getExistingForFilters(filters);
    assertEquals(25, Iterables.size(filtered));
  }

  @Test
  public void testApplyPlannedMutations() throws Exception {
    Table table = connection.getTable(TableName.valueOf(TABLE));

    Config config = ConfigUtils.configFromResource("/hbase/hbase-output-test.conf").getConfig("output");
    config = config.withValue("zookeeper",
        ConfigValueFactory.fromAnyRef("localhost:" + utility.getZkCluster().getClientPort()));

    HBaseOutput output = new HBaseOutput();
    output.configure(config);
    List<Row> records = createPlannedMutations();

    // Should be empty
    scanAndCountTable(table, 0);

    output.applyRandomMutations(records);

    HBaseSerde serde = HBaseUtils.getSerde(config);

    // Should be four cells per row
    scanAndCountTable(table, INPUT_ROWS * 4);
    List<Result> results = scanAndReturnTable(table);
    for (Result result : results) {
      Row row = serde.convertFromResult(result);
      assertEquals(6, row.schema().length());
      for (int i = 0; i < 6; i++) {
        assertNotNull(row.get(i));
      }
    }

    for (Row record : records) {
      // Turn it into a DELETE
      records.set(records.indexOf(record), PlannerUtils.setMutationType(record, MutationType.DELETE));
    }

    output.applyRandomMutations(records);

    // Should be empty
    scanAndCountTable(table, 0);
  }

  @Test
  public void testApplyBulkMutations() throws Exception {
    Table table = connection.getTable(TableName.valueOf(TABLE));

    Config config = ConfigUtils.configFromResource("/hbase/hbase-output-test.conf").getConfig("output");
    config = config.withValue("zookeeper",
        ConfigValueFactory.fromAnyRef("localhost:" + utility.getZkCluster().getClientPort()));

    HBaseOutput output = new HBaseOutput();
    output.configure(config);

    // Generate bulk mutations
    Dataset<Row> upserts = createBulkMutations(INPUT_ROWS);
    Dataset<Row> deletes = createBulkMutations(INPUT_ROWS);

    List<Tuple2<MutationType, Dataset<Row>>> bulk1 = Lists.newArrayList();
    bulk1.add(new Tuple2<>(MutationType.UPSERT, upserts));
    List<Tuple2<MutationType, Dataset<Row>>> bulk2 = Lists.newArrayList();
    bulk2.add(new Tuple2<>(MutationType.DELETE, deletes));
    List<Tuple2<MutationType, Dataset<Row>>> bulk3 = Lists.newArrayList();
    bulk3.add(new Tuple2<>(MutationType.UPSERT, upserts));
    bulk3.add(new Tuple2<>(MutationType.DELETE, deletes));

    // Run 1 should have 2000
    output.applyBulkMutations(bulk1);
    scanAndCountTable(table, INPUT_ROWS * 4);

    // Run 2 should have 0
    output.applyBulkMutations(bulk2);
    scanAndCountTable(table, 0);

    // Run 3 should have 0
    output.applyBulkMutations(bulk3);
    scanAndCountTable(table, 0);
  }

  private List<Result> scanAndReturnTable(Table table) throws IOException {
    List<Result> results = Lists.newArrayList();
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      results.add(result);
    }
    return results;
  }

  private void scanAndCountTable(Table table, int expected) throws IOException {
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    int count = 0;
    for (Result result : scanner) {
      CellScanner cellScanner = result.cellScanner();
      while (cellScanner.advance()) {
        count++;
      }
    }
    assertEquals(expected, count);
  }

}
