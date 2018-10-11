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

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDefaultHBaseSerde {

  private StructType filterSchema1 = new StructType(
      new StructField[]{
          new StructField("symbol", DataTypes.StringType, false, Metadata.empty()),
          new StructField("transacttime", DataTypes.LongType, false, Metadata.empty())
      }
  );

  private StructType filterSchema2 = new StructType(
      new StructField[]{
          new StructField("symbol", DataTypes.StringType, false, Metadata.empty()),
          new StructField("transacttime", DataTypes.LongType, false, Metadata.empty()),
          new StructField("clordid", DataTypes.StringType, false, Metadata.empty())
      }
  );
  
  private StructType filterSchemaPrefix = new StructType(
      new StructField[]{
          new StructField("symbol", DataTypes.StringType, false, Metadata.empty())
      }
  );

  private StructType fullSchema = new StructType(
      new StructField[]{
          new StructField("symbol", DataTypes.StringType, false, Metadata.empty()),
          new StructField("transacttime", DataTypes.LongType, false, Metadata.empty()),
          new StructField("clordid", DataTypes.StringType, false, Metadata.empty()),
          new StructField("orderqty", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("cumqty", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("leavesqty", DataTypes.IntegerType, false, Metadata.empty())
      }
  );

  private Config config = ConfigUtils.configFromResource("/hbase/hbase-output.conf").getConfig("output");
  private DefaultHBaseSerde serde;

  @Before
  public void before() {
    serde = new DefaultHBaseSerde();
    serde.configure(config);
  }

  @Test
  public void testConvertToGetRowKeyColumns() {
    Row filterRow = new RowWithSchema(filterSchema1, "GOOG", 1_000_000_000L);
    Get get = (Get)serde.convertToQuery(filterRow);

    byte[] row = get.getRow();
    assertEquals("Row Key should contain symbol and transacttime separated by :",
        "GOOG:" + new String(Bytes.toBytes(1_000_000_000L)), new String(row));

    Set<byte[]> families = get.familySet();
    assertEquals("Should be zero column families", 0, families.size());

    Filter filter = get.getFilter();
    assertNull("No filters", filter);
  }

  @Test
  public void testConvertToGetRowKeyAndColumnFilters() {
    Row filterRow = new RowWithSchema(filterSchema2, "GOOG", 1_000_000_000L, "abcd");
    Get get = (Get)serde.convertToQuery(filterRow);

    byte[] row = get.getRow();
    assertEquals("Row Key should contain symbol and transacttime separated by :",
        "GOOG:" + new String(Bytes.toBytes(1_000_000_000L)), new String(row));

    Set<byte[]> families = get.familySet();
    assertEquals("Should be one column family", 1, families.size());
    assertTrue("Column family should be cf1", families.contains("cf1".getBytes()));

    FilterList filter = (FilterList) get.getFilter();
    assertEquals("One filter", 1, filter.getFilters().size());
    assertArrayEquals("Filtering for cf1:clordid = abcd", ((SingleColumnValueFilter)filter.getFilters().get(0)).getFamily(), "cf1".getBytes());
    assertArrayEquals("Filtering for cf1:clordid = abcd", ((SingleColumnValueFilter)filter.getFilters().get(0)).getQualifier(), "clordid".getBytes());
    assertArrayEquals("Filtering for cf1:clordid = abcd", ((SingleColumnValueFilter)filter.getFilters().get(0)).getComparator().getValue(), "abcd".getBytes());
  }
  
  @Test
  public void testConvertToScanPrefixKey() {
    Row filterRow = new RowWithSchema(filterSchemaPrefix, "AAPL");
    Scan scan = (Scan)serde.convertToQuery(filterRow);
    
    assertEquals("Should be start row 'AAPL'", "AAPL", Bytes.toString(scan.getStartRow()));
    assertEquals("Should be stop row 'AAPM'", "AAPM", Bytes.toString(scan.getStopRow()));
  }

  @Test
  public void testConvertFromResult() {
    byte[] rowKey = Bytes.add(Bytes.toBytes("GOOG:"), Bytes.toBytes(1000L));
    byte[] cf = Bytes.toBytes("cf1");
    byte[] clordid = Bytes.toBytes("clordid");
    byte[] orderqty = Bytes.toBytes("orderqty");
    byte[] leavesqty = Bytes.toBytes("leavesqty");
    byte[] cumqty = Bytes.toBytes("cumqty");
    List<Cell> cells = Lists.newArrayList(
        (Cell)new KeyValue(rowKey, cf, clordid, 1000L, Bytes.toBytes("abcd")),
        new KeyValue(rowKey, cf, orderqty, 1000L, Bytes.toBytes(100)),
        new KeyValue(rowKey, cf, leavesqty, 1000L, Bytes.toBytes(10)),
        new KeyValue(rowKey, cf, cumqty, 1000L, Bytes.toBytes(5))
    );
    Result result = Result.create(cells);
    Row row = serde.convertFromResult(result);

    assertEquals("Symbol should be GOOG", "GOOG", row.<String>getAs("symbol"));
    assertEquals("Transacttime should be 1000L", 1000L, row.<Long>getAs("transacttime").longValue());
    assertEquals("Clordid should be abcd", "abcd", row.<String>getAs("clordid"));
    assertEquals("Orderqty should be 100", 100, row.<Integer>getAs("orderqty").intValue());
    assertEquals("Leavesqty should be 10", 10, row.<Integer>getAs("leavesqty").intValue());
    assertEquals("Cumqty should be 5", 5, row.<Integer>getAs("cumqty").intValue());
  }

  @Test
  public void testConvertFromResults() {
    byte[] rowKey = Bytes.add(Bytes.toBytes("GOOG:"), Bytes.toBytes(1000L));
    byte[] cf = Bytes.toBytes("cf1");
    byte[] clordid = Bytes.toBytes("clordid");
    byte[] orderqty = Bytes.toBytes("orderqty");
    byte[] leavesqty = Bytes.toBytes("leavesqty");
    byte[] cumqty = Bytes.toBytes("cumqty");
    List<Cell> cells1 = Lists.newArrayList(
        (Cell)new KeyValue(rowKey, cf, clordid, 1000L, Bytes.toBytes("abcd")),
        new KeyValue(rowKey, cf, orderqty, 1000L, Bytes.toBytes(100)),
        new KeyValue(rowKey, cf, leavesqty, 1000L, Bytes.toBytes(10)),
        new KeyValue(rowKey, cf, cumqty, 1000L, Bytes.toBytes(5))
    );
    Result result1 = Result.create(cells1);
    byte[] rowKey2 = Bytes.add(Bytes.toBytes("AAPL:"), Bytes.toBytes(1004L));
    List<Cell> cells2 = Lists.newArrayList(
        (Cell)new KeyValue(rowKey2, cf, clordid, 1004L, Bytes.toBytes("efgh")),
        new KeyValue(rowKey2, cf, orderqty, 1004L, Bytes.toBytes(99)),
        new KeyValue(rowKey2, cf, leavesqty, 1004L, Bytes.toBytes(9)),
        new KeyValue(rowKey2, cf, cumqty, 1004L, Bytes.toBytes(4))
    );
    Result result2 = Result.create(cells2);

    List<Row> rows = serde.convertFromResults(Lists.newArrayList(result1, result2));

    assertEquals("Two Rows should be returned", 2, rows.size());
    assertEquals("Symbol should be GOOG", "GOOG", rows.get(0).<String>getAs("symbol"));
    assertEquals("Transacttime should be 1000L", 1000L, rows.get(0).<Long>getAs("transacttime").longValue());
    assertEquals("Clordid should be abcd", "abcd", rows.get(0).<String>getAs("clordid"));
    assertEquals("Orderqty should be 100", 100, rows.get(0).<Integer>getAs("orderqty").intValue());
    assertEquals("Leavesqty should be 10", 10, rows.get(0).<Integer>getAs("leavesqty").intValue());
    assertEquals("Cumqty should be 5", 5, rows.get(0).<Integer>getAs("cumqty").intValue());

    assertEquals("Symbol should be AAPL", "AAPL", rows.get(1).<String>getAs("symbol"));
    assertEquals("Transacttime should be 1004L", 1004L, rows.get(1).<Long>getAs("transacttime").longValue());
    assertEquals("Clordid should be efgh", "efgh", rows.get(1).<String>getAs("clordid"));
    assertEquals("Orderqty should be 99", 99, rows.get(1).<Integer>getAs("orderqty").intValue());
    assertEquals("Leavesqty should be 9", 9, rows.get(1).<Integer>getAs("leavesqty").intValue());
    assertEquals("Cumqty should be 4", 4, rows.get(1).<Integer>getAs("cumqty").intValue());
  }

  @Test
  public void testConvertToPut() {
    byte[] rowKey = Bytes.add(Bytes.toBytes("GOOG:"), Bytes.toBytes(1000L));
    byte[] cf = Bytes.toBytes("cf1");
    byte[] clordid = Bytes.toBytes("clordid");
    byte[] orderqty = Bytes.toBytes("orderqty");
    byte[] leavesqty = Bytes.toBytes("leavesqty");
    byte[] cumqty = Bytes.toBytes("cumqty");
    Row row = new RowWithSchema(fullSchema, "GOOG", 1000L, "abcd", 100, 10, 5);
    Put put = serde.convertToPut(row);

    assertArrayEquals("Row key should be GOOG:1000L", rowKey, put.getRow());
    assertTrue("cf1:clordid should be abcd", put.has(cf, clordid, Bytes.toBytes("abcd")));
    assertTrue("cf1:orderqty should be 100", put.has(cf, orderqty, Bytes.toBytes(100)));
    assertTrue("cf1:leavesqty should be 5", put.has(cf, leavesqty, Bytes.toBytes(5)));
    assertTrue("cf1:cumqty should be 10", put.has(cf, cumqty, Bytes.toBytes(10)));
  }

  @Test
  public void testConvertToPuts() {
    byte[] cf = Bytes.toBytes("cf1");
    byte[] clordid = Bytes.toBytes("clordid");
    byte[] orderqty = Bytes.toBytes("orderqty");
    byte[] leavesqty = Bytes.toBytes("leavesqty");
    byte[] cumqty = Bytes.toBytes("cumqty");
    List<Row> rows = Lists.newArrayList(
        (Row)new RowWithSchema(fullSchema, "GOOG", 1000L, "abcd", 100, 10, 5),
        new RowWithSchema(fullSchema, "AAPL", 1000L, "efgh", 99, 9, 4)
    );
    List<Put> puts = serde.convertToPuts(rows);

    assertEquals("Puts length should be 2", 2, puts.size());
    assertArrayEquals("Row key should be GOOG:1000L",
        Bytes.add(Bytes.toBytes("GOOG:"), Bytes.toBytes(1000L)), puts.get(0).getRow());
    assertTrue("cf1:clordid should be abcd", puts.get(0).has(cf, clordid, Bytes.toBytes("abcd")));
    assertTrue("cf1:orderqty should be 100", puts.get(0).has(cf, orderqty, Bytes.toBytes(100)));
    assertTrue("cf1:leavesqty should be 5", puts.get(0).has(cf, leavesqty, Bytes.toBytes(5)));
    assertTrue("cf1:cumqty should be 10", puts.get(0).has(cf, cumqty, Bytes.toBytes(10)));

    assertArrayEquals("Row key should be AAPL:1000L",
        Bytes.add(Bytes.toBytes("AAPL:"), Bytes.toBytes(1000L)), puts.get(1).getRow());
    assertTrue("cf1:clordid should be efgh", puts.get(1).has(cf, clordid, Bytes.toBytes("efgh")));
    assertTrue("cf1:orderqty should be 99", puts.get(1).has(cf, orderqty, Bytes.toBytes(99)));
    assertTrue("cf1:leavesqty should be 4", puts.get(1).has(cf, leavesqty, Bytes.toBytes(4)));
    assertTrue("cf1:cumqty should be 9", puts.get(1).has(cf, cumqty, Bytes.toBytes(9)));
  }

  @Test
  public void testConvertToDelete() {
    byte[] rowKey = Bytes.add(Bytes.toBytes("GOOG:"), Bytes.toBytes(1000L));
    byte[] cf = Bytes.toBytes("cf1");
    byte[] clordid = Bytes.toBytes("clordid");
    byte[] orderqty = Bytes.toBytes("orderqty");
    byte[] leavesqty = Bytes.toBytes("leavesqty");
    byte[] cumqty = Bytes.toBytes("cumqty");
    Row row = new RowWithSchema(fullSchema, "GOOG", 1000L, "abcd", 100, 10, 5);
    Delete delete = serde.convertToDelete(row);

    Map<byte[], List<Cell>> contents = delete.getFamilyCellMap();

    assertArrayEquals("Row key should be GOOG:1000L", rowKey, delete.getRow());
    assertTrue("Delete contains cf1", contents.containsKey(cf));
    List<Cell> cells = contents.get(cf);
    assertEquals("Delete should have four cells", 4, cells.size());
    assertArrayEquals("Cell 0 should be cf1:clordid", clordid, CellUtil.cloneQualifier(cells.get(0)));
    assertArrayEquals("Cell 1 should be cf1:cumqty", cumqty, CellUtil.cloneQualifier(cells.get(1)));
    assertArrayEquals("Cell 2 should be cf1:leavesqty", leavesqty, CellUtil.cloneQualifier(cells.get(2)));
    assertArrayEquals("Cell 3 should be cf1:orderqty", orderqty, CellUtil.cloneQualifier(cells.get(3)));
  }

  @Test
  public void testConvertToDeletes() {
    byte[] cf = Bytes.toBytes("cf1");
    byte[] clordid = Bytes.toBytes("clordid");
    byte[] orderqty = Bytes.toBytes("orderqty");
    byte[] leavesqty = Bytes.toBytes("leavesqty");
    byte[] cumqty = Bytes.toBytes("cumqty");
    List<Row> rows = Lists.newArrayList(
      (Row)new RowWithSchema(fullSchema, "GOOG", 1000L, "abcd", 100, 10, 5),
      new RowWithSchema(fullSchema, "AAPL", 1000L, "efgh", 99, 9, 4)
    );
    List<Delete> deletes = serde.convertToDeletes(rows);

    byte[] rowKey1 = Bytes.add(Bytes.toBytes("GOOG:"), Bytes.toBytes(1000L));
    assertEquals("Should be 2 deletes", 2, deletes.size());
    Map<byte[], List<Cell>> contents1 = deletes.get(0).getFamilyCellMap();
    assertArrayEquals("Row key should be GOOG:1000L", rowKey1, deletes.get(0).getRow());
    assertTrue("Delete contains cf1", contents1.containsKey(cf));
    List<Cell> cells1 = contents1.get(cf);
    assertEquals("Delete should have four cells", 4, cells1.size());
    assertArrayEquals("Cell 0 should be cf1:clordid", clordid, CellUtil.cloneQualifier(cells1.get(0)));
    assertArrayEquals("Cell 1 should be cf1:cumqty", cumqty, CellUtil.cloneQualifier(cells1.get(1)));
    assertArrayEquals("Cell 2 should be cf1:leavesqty", leavesqty, CellUtil.cloneQualifier(cells1.get(2)));
    assertArrayEquals("Cell 3 should be cf1:orderqty", orderqty, CellUtil.cloneQualifier(cells1.get(3)));

    byte[] rowKey2 = Bytes.add(Bytes.toBytes("AAPL:"), Bytes.toBytes(1000L));
    assertEquals("Should be 2 deletes", 2, deletes.size());
    Map<byte[], List<Cell>> contents2 = deletes.get(1).getFamilyCellMap();
    assertArrayEquals("Row key should be GOOG:1000L", rowKey2, deletes.get(1).getRow());
    assertTrue("Delete contains cf1", contents2.containsKey(cf));
    List<Cell> cells2 = contents2.get(cf);
    assertEquals("Delete should have four cells", 4, cells2.size());
    assertArrayEquals("Cell 0 should be cf1:clordid", clordid, CellUtil.cloneQualifier(cells2.get(0)));
    assertArrayEquals("Cell 1 should be cf1:cumqty", cumqty, CellUtil.cloneQualifier(cells2.get(1)));
    assertArrayEquals("Cell 2 should be cf1:leavesqty", leavesqty, CellUtil.cloneQualifier(cells2.get(2)));
    assertArrayEquals("Cell 3 should be cf1:orderqty", orderqty, CellUtil.cloneQualifier(cells2.get(3)));
  }

}
