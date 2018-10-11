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

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.output.BulkOutput;
import com.cloudera.labs.envelope.output.RandomOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.utils.PlannerUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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
public class HBaseOutput implements RandomOutput, BulkOutput, ProvidesAlias, ProvidesValidations {

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
    this.tableName = HBaseUtils.tableInfoFor(config);
    this.batchSize = HBaseUtils.batchSizeFor(config);
  }

  @Override
  public Iterable<Row> getExistingForFilters(Iterable<Row> filters) throws Exception {
    LOG.debug("Fetching filter rows from table: {}", tableName.toString());
    List<Row> filterResults = Lists.newArrayList();
    try (Table table = getConnection(config).getTable(tableName)) {
      List<Get> gets = Lists.newArrayList();
      List<Scan> scans = Lists.newArrayList();

      for (Row filter : filters) {
        // Construct row key from key columns
        Query query = getSerde(config).convertToQuery(filter);
        LOG.debug("Adding filter: {}", query);
        
        if (query instanceof Get) {
          gets.add((Get)query);
        }
        else if (query instanceof Scan) {
          scans.add((Scan)query);
        }
        else {
          throw new RuntimeException("Unsupported HBase query class: " + query.getClass().getName());
        }
      }
      
      List<Result> results = Lists.newArrayList();
      if (gets.size() > 0) {
        results.addAll(Lists.newArrayList(table.get(gets)));
      }
      if (scans.size() > 0) {
        Scan mergedScan = HBaseUtils.mergeRangeScans(scans);
        results.addAll(Lists.newArrayList(table.getScanner(mergedScan)));
      }
      
      filterResults.addAll(getSerde(config).convertFromResults(results));
    }

    return filterResults;
  }

  @Override
  public void applyRandomMutations(List<Row> plannedRows) throws Exception {
    LOG.debug("Applying planned rows to table: {}", tableName.toString());
    try (Table table = getConnection(config).getTable(tableName)) {
      List<Mutation> actions = Lists.newArrayList();

      LOG.debug("Extracting mutations from {} rows", plannedRows.size());

      for (Row row : plannedRows) {
        MutationType mutationType = PlannerUtils.getMutationType(row);
        row = PlannerUtils.removeMutationTypeField(row);
        switch (mutationType) {
          case UPSERT:
            actions.add(getSerde(config).convertToPut(row));
            break;
          case DELETE:
            actions.add(getSerde(config).convertToDelete(row));
            break;
          default:
            throw new RuntimeException("Unsupported HBase mutation type: " +
                PlannerUtils.getMutationType(row));
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

  @Override
  public String getAlias() {
    return "hbase";
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

  @Override
  public Validations getValidations() {
    return HBaseUtils.getValidations();
  }
  
}
