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
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultHBaseSerde implements HBaseSerde {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultHBaseSerde.class);

  private List<String> keyColumns;
  private Map<String, ColumnDef> columns;
  private StructType schema;
  private byte[] keySeparator;

  public void configure(Config config) {
    this.keyColumns = HBaseUtils.rowKeyFor(config);
    this.columns = HBaseUtils.columnsFor(config);
    this.schema = HBaseUtils.buildSchema(columns);
    this.keySeparator = HBaseUtils.rowKeySeparatorFor(config);
  }

  @Override
  public Query convertToQuery(Row row) {
    if (filtersEntireRowKey(row)) {
      return convertToGet(row);
    }
    else if (filtersRowKeyPrefix(row)) {
      return convertToScan(row);
    }
    else {
      throw new RuntimeException("Default HBase serde only supports full row key or prefix row key reads.");
    }
  }

  @Override
  public Row convertFromResult(Result result) {
    // TODO support a more sophisticated approach that does not assume row key uniqueness

    // Initial array of nulls
    Object[] values = new Object[schema.length()];

    // Get row key fields
    byte[] rowKey = result.getRow();
    int index = 0;
    for (int i = 0; i < keyColumns.size(); i++) {
      ColumnDef def = columns.get(keyColumns.get(i));
      index += addColumnValue(rowKey, index, rowKey.length, values,
          def.type, schema.fieldIndex(def.name), keySeparator, i == keyColumns.size() - 1);
      if (i < keyColumns.size() - 1) {
        // increment by delimiter length
        index += keySeparator.length;
      }
    }

    // Get columns
    for (Cell cell : result.listCells()) {
      String cellName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
          cell.getQualifierLength());
      ColumnDef def = columns.get(cellName);
      values[schema.fieldIndex(def.name)] = getColumnValue(cell.getValueArray(),
          cell.getValueOffset(), cell.getValueLength(), def.type);
    }

    return new RowWithSchema(schema, values);
  }

  @Override
  public List<Row> convertFromResults(Iterable<Result> results) {
    List<Row> rows = Lists.newArrayList();
    for (Result result : results) {
      rows.add(convertFromResult(result));
    }

    return rows;
  }

  @Override
  public Put convertToPut(Row row) {
    Put put = new Put(buildRowKey(row));
    for (Map.Entry<String, ColumnDef> column : columns.entrySet()) {
      if (!column.getValue().cf.equals("rowkey")) {
        byte[] value = getColumnValueAsBytes(column.getValue().name,
            column.getValue().type, row);
        if (value != null) {
          put.addColumn(Bytes.toBytes(column.getValue().cf),
              Bytes.toBytes(column.getValue().name), value);
        }
      }
    }
    return put;
  }

  @Override
  public List<Put> convertToPuts(List<Row> rows) {
    List<Put> puts = Lists.newArrayList();
    for (Row row : rows) {
      puts.add(convertToPut(row));
    }
    return puts;
  }

  @Override
  public Delete convertToDelete(Row row) {
    Delete delete = new Delete(buildRowKey(row));
    for (Map.Entry<String, ColumnDef> column : columns.entrySet()) {
      if (!column.getValue().cf.equals("rowkey")) {
        delete.addColumn(Bytes.toBytes(column.getValue().cf),
            Bytes.toBytes(column.getValue().name));
      }
    }
    return delete;
  }

  @Override
  public List<Delete> convertToDeletes(List<Row> rows) {
    List<Delete> deletes = Lists.newArrayList();
    for (Row row : rows) {
      deletes.add(convertToDelete(row));
    }
    return deletes;
  }

  //// Utility methods

  private Get convertToGet(Row row) {
    Get get = new Get(buildRowKey(row));
    for (String family : getColumnFamilies(row)) {
      get.addFamily(Bytes.toBytes(family));
    }

    FilterList filters = getColumnValueFilters(row);
    if (!filters.getFilters().isEmpty()) {
      get.setFilter(filters);
    }
    
    return get;
  }
  
  private Scan convertToScan(Row row) {
    byte[] startRow = buildRowKey(row);
    byte[] stopRow = HBaseUtils.exclusiveStopRow(startRow);
    Scan scan = new Scan(startRow, stopRow);
    
    return scan;
  }
  
  private boolean filtersEntireRowKey(Row row) {
    for (String keyColumn : keyColumns) {
      if (!Arrays.asList(row.schema().fieldNames()).contains(keyColumn)) {
        return false;
      }
    }
    
    return true;
  }
  
  private boolean filtersRowKeyPrefix(Row row) {
    Set<String> rowColumnNames = Sets.newHashSet(row.schema().fieldNames());
    Set<String> prefixColumnNames = Sets.newHashSet(keyColumns.subList(0, rowColumnNames.size()));
    
    return rowColumnNames.equals(prefixColumnNames);
  }
  
  private byte[] buildRowKey(Row row) {
    List<byte[]> keyComponents = Lists.newArrayList();
    int totalSize = 0;
    List<String> rowColumns = Arrays.asList(row.schema().fieldNames());
    for (String keyColumn : keyColumns) {
      if (!rowColumns.contains(keyColumn)) {
        break;
      }
      ColumnDef def = columns.get(keyColumn);
      byte[] asBytes = getColumnValueAsBytes(def.name, def.type, row);
      keyComponents.add(asBytes);
      totalSize += asBytes.length;
    }

    byte[] fullRow = new byte[totalSize + ((keyComponents.size() - 1) * keySeparator.length)];
    int currentOffset = 0;
    for (int i = 0; i < keyComponents.size(); ++i) {
      byte[] component = keyComponents.get(i);
      System.arraycopy(component, 0, fullRow, currentOffset, component.length);
      currentOffset += component.length;
      if (i < keyComponents.size() - 1) {
        System.arraycopy(keySeparator, 0, fullRow, currentOffset, keySeparator.length);
        currentOffset += keySeparator.length;
      }
    }

    return fullRow;
  }
  
  private Set<String> getColumnFamilies(Row row) {
    Set<String> families = Sets.newHashSet();
    
    for (String fieldName : row.schema().fieldNames()) {
      ColumnDef def = columns.get(fieldName);
      if (!def.cf.equals("rowkey")) {
        families.add(def.cf);
      }
    }
    
    return families;
  }

  private static Object getColumnValue(byte[] source, int offset, int length, String type) {
    switch (type) {
      case "int":
        return Bytes.toInt(source, offset, length);
      case "long":
        return Bytes.toLong(source, offset, length);
      case "boolean":
        return Bytes.toBoolean(source);
      case "float":
        return Bytes.toFloat(source);
      case "double":
        return Bytes.toDouble(source);
      case "string":
        return Bytes.toString(source, offset, length);
      default:
        LOG.error("Unsupported column type: {}", type);
        throw new IllegalArgumentException("Unsupported column type: " + type);
    }
  }

  private static int addColumnValue(byte[] source, int offset, int endIndex,
                                    Object[] values, String type, int valueIndex, byte[] keySeparator, boolean last) {
    switch (type) {
      case "int":
        values[valueIndex] = Bytes.toInt(source, offset, 4);
        return 4;
      case "long":
        values[valueIndex] = Bytes.toLong(source, offset, 8);
        return 8;
      case "boolean":
        values[valueIndex] = Bytes.toInt(source, offset, 1);
        return 1;
      case "float":
        values[valueIndex] = Bytes.toFloat(source, offset);
        return 4;
      case "double":
        values[valueIndex] = Bytes.toDouble(source, offset);
        return 8;
      case "string":
        if (last) {
          // if the last field just grab it all
          values[valueIndex] = Bytes.toString(source, offset, endIndex - offset);
          return endIndex - offset;
        } else {
          int startIndex = offset;
          while (offset < endIndex) {
            if (source[offset] != keySeparator[0]) {
              offset++;
            } else {
              // Might be the start of a separator
              int startOfOffset = offset;
              int sepOffset = 1;
              boolean isSep = sepOffset == keySeparator.length;
              while (sepOffset < keySeparator.length && offset < endIndex &&
                  source[offset] == keySeparator[sepOffset]) {
                isSep = sepOffset == keySeparator.length - 1;
                offset++;
                sepOffset++;
              }
              if (isSep) {
                // We found a separator, so return the string before that
                values[valueIndex] = Bytes.toString(source, startIndex, startOfOffset - startIndex);
                return startOfOffset - startIndex;
              }
            }
          }
          // We reached the end which is an error except for the last field
          if (offset == endIndex - 1) {
            LOG.error("Reached end of array while looking for separator");
            throw new IllegalArgumentException("Reached end of array while looking for separator");
          } else {
            values[valueIndex] = Bytes.toString(source, startIndex, offset - startIndex);
            return offset - startIndex;
          }
        }
      default:
        LOG.error("Unsupported column type: {}", type);
        throw new IllegalArgumentException("Unsupported column type: " + type);
    }
  }

  private static byte[] getColumnValueAsBytes(String name, String type, Row row) {
    try {
      Object field = RowUtils.get(row, name);
      if (field == null) {
        return null;
      }
      switch (type) {
        case "string":
          return Bytes.toBytes((String) RowUtils.get(row, name));
        case "int":
          return Bytes.toBytes((int) RowUtils.get(row, name));
        case "long":
          return Bytes.toBytes((long) RowUtils.get(row, name));
        case "float":
          return Bytes.toBytes((float) RowUtils.get(row, name));
        case "double":
          return Bytes.toBytes((double) RowUtils.get(row, name));
        case "boolean":
          return Bytes.toBytes((boolean) RowUtils.get(row, name));
        default:
          LOG.error("Unsupported column type: {}", type);
          throw new IllegalArgumentException("Unsupported column type: " + type);
      }
    } catch (IllegalArgumentException e) {
      LOG.error("Column does not exist in row: " + name);
      throw e;
    }
  }
  
  private FilterList getColumnValueFilters(Row row) {
    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
    Set<String> filterColumnNames = Sets.newHashSet(row.schema().fieldNames());
    
    for (Map.Entry<String, ColumnDef> column : columns.entrySet()) {
      if (!column.getValue().cf.equals("rowkey")) {
        if (filterColumnNames.contains(column.getKey())) {
          byte[] value = getColumnValueAsBytes(column.getValue().name, column.getValue().type, row);
          if (value != null) {
            SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes(column.getValue().cf),
                Bytes.toBytes(column.getValue().name),
                CompareFilter.CompareOp.EQUAL,
                value
            );
            filterList.addFilter(columnValueFilter);
          }
        }
      }
    }
    
    return filterList;
  }

}
