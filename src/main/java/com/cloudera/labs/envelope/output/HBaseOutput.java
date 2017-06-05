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
package com.cloudera.labs.envelope.output;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.cloudera.labs.envelope.utils.hbase.HBaseUtils;
import com.cloudera.labs.envelope.utils.hbase.HBaseSerde;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * HBase output implementing the RandomOutput and BulkOutput APIs.
 * <p>
 * The output comes with a default serde implementation DefaultHBaseSerde which reads in a
 * column mapping configuration to convert to and from SparkSQL Rows with StructType schemas and
 * HBase cells. The serde approach is pluggable but currently only one implementation is provided.
 * <p>
 * The default serde configuration syntax adheres as closely as possible to that of the
 * Spark-HBase DataSource at the expense of some additional functionality - this is with a view to
 * moving to the HBaseRelation at some point in the future. An example can be found below:
 * <p>
 * <pre>
 *     type = hbase
 *     zookeeper = "vm1:2181"
 *     table.name = "default:test"
 *     mapping {
 *       rowkey = ["symbol", "transacttime"]
 *       columns {
 *         symbol {
 *           cf = "rowkey"
 *           col = "symbol"
 *           type = "string"
 *         }
 *         transacttime {
 *           cf = "rowkey"
 *           col = "transacttime"
 *           type = "long"
 *         }
 *         clordid {
 *           cf = "cf1"
 *           col = "clordid"
 *           type = "string"
 *         }
 *         orderqty {
 *           cf = "cf1"
 *           col = "orderqty"
 *           type = "int"
 *         }
 *         leavesqty {
 *           cf = "cf1"
 *           col = "leavesqty"
 *           type = "int"
 *         }
 *         cumqty {
 *           cf = "cf1"
 *           col = "cumqty"
 *           type = "int"
 *         }
 *       }
 *     }
 * </pre>
 */
public class HBaseOutput implements RandomOutput, BulkOutput {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseOutput.class);

  private Config config;

  // HBase connection and properties
  private static Connection connection;
  private static HBaseSerde serde;
  private TableName tableName;
  private int batchSize;

  // API methods

  @Override
  public void configure(Config config) {
    this.config = config;
    if (HBaseUtils.validateConfig(config)) {
      tableName = HBaseUtils.tableInfoFor(config);
      batchSize = HBaseUtils.batchSizeFor(config);
    } else {
      LOG.error("Invalid configuration");
      throw new IllegalArgumentException("Invalid configuration");
    }
  }

  @Override
  public Iterable<Row> getExistingForFilters(Iterable<Row> filters) throws Exception {
    LOG.debug("Fetching filter rows from table: {}", tableName.toString());
    List<Row> filterResults = Lists.newArrayList();
    try (Table table = getConnection(config).getTable(tableName)) {
      // Options here: iterate over filters and produce a sub-set of HBase requests using
      // MultiRowRangeFilter (harder), or issue one request per filter (easier). In the
      // initial
      // case go with the naïve approach but in the future look for optimizations based on
      // combining row key ranges and column filters.

      // TODO support more advanced scans based on filters
      List<Get> gets = Lists.newArrayList();

      for (Row filter : filters) {
        // Construct row key from key columns
        Get get = getSerde(config).convertToGet(filter);
        LOG.debug("Adding filter: {}", get);
        gets.add(get);
      }

      Result[] results = table.get(gets);
      filterResults.addAll(getSerde(config).convertFromResults(results));
    }

    return filterResults;
  }

  @Override
  public void applyRandomMutations(List<PlannedRow> plannedRows) throws Exception {
    LOG.debug("Applying planned rows to table: {}", tableName.toString());
    try (Table table = getConnection(config).getTable(tableName)) {
      List<Mutation> actions = Lists.newArrayList();

      LOG.debug("Extracting mutations from {} rows", plannedRows.size());

      for (PlannedRow row : plannedRows) {
        switch (row.getMutationType()) {
          case UPSERT:
            actions.add(getSerde(config).convertToPut(row.getRow()));
            break;
          case DELETE:
            actions.add(getSerde(config).convertToDelete(row.getRow()));
            break;
          default:
            throw new RuntimeException("Unsupported HBase mutation type: " +
                row.getMutationType().toString());
        }

        if (actions.size() >= batchSize) {
          // TODO: Should probably check the results
          LOG.debug("Applying {} rows in batch", actions.size());
          Object[] results = new Object[actions.size()];
          table.batch(actions, results);
          actions.clear();
        }
      }
      if (actions.size() > 0) {
        LOG.debug("Applying {} rows in final batch", actions.size());
        Object[] results = new Object[actions.size()];
        table.batch(actions, results);
        LOG.debug("Batch complete");
      }
    }
  }

  @Override
  public Set<MutationType> getSupportedRandomMutationTypes() {
    return Sets.newHashSet(MutationType.UPSERT, MutationType.DELETE);
  }

  @Override
  public Set<MutationType> getSupportedBulkMutationTypes() {
    return Sets.newHashSet(MutationType.UPSERT, MutationType.DELETE);
  }

  @Override
  public void applyBulkMutations(List<Tuple2<MutationType, Dataset<Row>>> planned) {
    for (Tuple2<MutationType, Dataset<Row>> mutationDataset : planned) {
      BulkHBaseMutatorFunction mutatorFunction;
      switch (mutationDataset._1()) {
        case UPSERT:
        case DELETE:
          mutatorFunction = new BulkHBaseMutatorFunction(config, mutationDataset._1());
          break;
        default:
          LOG.error("Unsupported bulk mutation type: {}", mutationDataset._1());
          throw new RuntimeException("Unsupported bulk mutation type: " + mutationDataset._1());
      }
      // Run the function against the DF
      mutationDataset._2().foreachPartition(mutatorFunction);
    }
  }

  @SuppressWarnings("serial")
  public static class BulkHBaseMutatorFunction implements ForeachPartitionFunction<Row> {

    private Config _config;
    private MutationType _mutationType;
    private transient HBaseSerde serde;
    private transient Function<Row, ? extends Mutation> _rowToMutation;
    private transient Connection connection;

    BulkHBaseMutatorFunction(Config config, MutationType mutationType) {
      this._config = config;
      this._mutationType = mutationType;
    }

    @Override
    public void call(Iterator<Row> iter) throws Exception {
      if (connection == null) {
        try {
          connection = getConnection(_config);
        } catch (IOException e) {
          LOG.error("Could not obtain HBase connection: " + e.getMessage());
          throw e;
        }
      }

      if (_rowToMutation == null) {
        serde = HBaseUtils.getSerde(_config);
        switch (_mutationType) {
          case UPSERT:
            _rowToMutation = new HBaseSerde.RowToPut(serde);
            break;
          case DELETE:
            _rowToMutation = new HBaseSerde.RowToDelete(serde);
            break;
          default:
            LOG.error("Unsupported bulk mutation type: {}", _mutationType);
            throw new RuntimeException("Unsupported bulk mutation type: " + _mutationType);
        }
      }

      try (BufferedMutator mutator = connection.getBufferedMutator(HBaseUtils.tableInfoFor(_config))) {
        while (iter.hasNext()) {
          Row r = iter.next();
          mutator.mutate(_rowToMutation.call(r));
        }
        mutator.flush();
      } catch (Exception e) {
        LOG.error("Could not process row: " + e.getMessage());
        throw e;
      }
    }
  }

  private static synchronized HBaseSerde getSerde(Config config) {
    if (serde == null) {
      serde = HBaseUtils.getSerde(config);
    }
    return serde;
  }

  private static synchronized Connection getConnection(Config config) throws IOException {
    if (connection == null) {
      connection = HBaseUtils.getConnection(config);
    }
    return connection;
  }
}
