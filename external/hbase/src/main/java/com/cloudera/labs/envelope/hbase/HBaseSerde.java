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

import com.typesafe.config.Config;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * Interface defining methods to convert to/from HBase cells from/to Spark {@link Row}s.
 */
public interface HBaseSerde {

  /**
   * Simple Column specification for HBase columns
   */
  class ColumnDef {

    public final String cf;
    public final String name;
    public final String type;

    public ColumnDef(String cf, String name, String type) {
      this.cf = cf;
      this.name = name;
      this.type = type;
    }

  }

  /**
   * Function to convert from {@link Row} to {@link Put}
   */
  class RowToPut implements Function<Row, Put> {

    private HBaseSerde serde;

    public RowToPut(HBaseSerde serde) {
      this.serde = serde;
    }

    @Override
    public Put call(Row row) throws Exception {
      return serde.convertToPut(row);
    }

  }

  /**
   * Function to convert from {@link Row} to {@link Delete}
   */
  class RowToDelete implements Function<Row, Delete> {

    private HBaseSerde serde;

    public RowToDelete(HBaseSerde serde) {
      this.serde = serde;
    }

    @Override
    public Delete call(Row row) throws Exception {
      return serde.convertToDelete(row);
    }

  }

  /**
   * Function to convert from {@link Row} to {@link Query}
   */
  class RowToQuery implements Function<Row, Query> {

    private HBaseSerde serde;

    public RowToQuery(HBaseSerde serde) {
      this.serde = serde;
    }

    @Override
    public Query call(Row row) throws Exception {
      return serde.convertToQuery(row);
    }

  }

  /**
   * Configure the Serde.
   * This will be passed the contents of an "input" or "output" configuration section.
   * @param config
   */
  void configure(Config config);

  /**
   * Convert the given {@link Row} to a {@link Query}
   * @param row
   * @return a {@link Query}
   */
  Query convertToQuery(Row row);

  /**
   * Convert the given HBase {@link Result} to a {@link Row}
   * @param result
   * @return a {@link Row}
   */
  Row convertFromResult(Result result);

  /**
   * Convert the given {@link Row} to a {@link Put}
   * @param row
   * @return a {@link Put}
   */
  Put convertToPut(Row row);

  /**
   * Convert the given {@link Row} to a {@link Delete}
   * All columns in the Row are mapped to Deletes but the only _values_
   * of row columns considered are those of row key columns
   * @param row
   * @return a {@link Delete}
   */
  Delete convertToDelete(Row row);

  /**
   * Convert an iterable of {@link Result} objects to a list of {@link Row}s
   * @param results
   * @return a list of {@link Row}
   */
  List<Row> convertFromResults(Iterable<Result> results);

  /**
   * Convert a list of {@link Row}s to a list of {@link Put}s
   * @param rows
   * @return a list of {@link Put}s
   */
  List<Put> convertToPuts(List<Row> rows);

  /**
   * Convert a list of {@link Row}s to HBase {@link Delete}s
   * @param rows
   * @return a list of {@link Delete}s
   */
  List<Delete> convertToDeletes(List<Row> rows);

}
