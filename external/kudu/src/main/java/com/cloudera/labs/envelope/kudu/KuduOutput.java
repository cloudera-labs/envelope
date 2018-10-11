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

package com.cloudera.labs.envelope.kudu;

import static com.cloudera.labs.envelope.kudu.KuduUtils.IGNORE_MISSING_COLUMNS_CONFIG_NAME;
import static com.cloudera.labs.envelope.kudu.KuduUtils.INSERT_IGNORE_CONFIG_NAME;
import static com.cloudera.labs.envelope.kudu.KuduUtils.IS_SECURE_CONFIG_NAME;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.output.BulkOutput;
import com.cloudera.labs.envelope.output.RandomOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.security.SecurityUtils;
import com.cloudera.labs.envelope.security.TokenProvider;
import com.cloudera.labs.envelope.security.UsesDelegationTokens;
import com.cloudera.labs.envelope.spark.AccumulatorRequest;
import com.cloudera.labs.envelope.spark.Accumulators;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.spark.UsesAccumulators;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.PlannerUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.kudu.spark.kudu.KuduRelation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class KuduOutput implements RandomOutput, BulkOutput, UsesAccumulators, ProvidesAlias, ProvidesValidations,
    UsesDelegationTokens {

  public static final String CONNECTION_CONFIG_NAME = "connection";
  public static final String TABLE_CONFIG_NAME = "table.name";
  
  private static final String ACCUMULATOR_NUMBER_OF_SCANNERS = "Number of Kudu scanners";
  private static final String ACCUMULATOR_NUMBER_OF_FILTERS_SCANNED = "Number of filters scanned in Kudu";
  private static final String ACCUMULATOR_SECONDS_SCANNING = "Seconds spent scanning Kudu";

  private Config config;
  private Accumulators accumulators;

  private Map<String, StructType> tableSchemas;

  private static Logger LOG = LoggerFactory.getLogger(KuduOutput.class);

  @Override
  public void configure(Config config) {
    this.config = config;
  }

  @Override
  public TokenProvider getTokenProvider() {
    if (KuduUtils.isSecure(config)) {
      return new KuduTokenProvider(config.getString(CONNECTION_CONFIG_NAME), config);
    } else {
      return null;
    }
  }

  @Override
  public void applyRandomMutations(List<Row> planned) throws Exception {
    KuduConnection connection = getConnection();
    KuduSession session = connection.getSession();
    KuduTable table = connection.getTable(config.getString(TABLE_CONFIG_NAME));

    List<Operation> operations = extractOperations(planned, table);

    for (Operation operation : operations) {
      session.apply(operation);
    }

    // Wait until all operations have completed before checking for errors.
    while (session.hasPendingOperations()) {
      Thread.sleep(1);
    }

    // Fail fast on any error applying mutations
    if (session.countPendingErrors() > 0) {
      RowError firstError = session.getPendingErrors().getRowErrors()[0];
      String errorMessage = String.format("Kudu output error '%s' during operation '%s' at tablet server '%s'",
          firstError.getErrorStatus(), firstError.getOperation(), firstError.getTsUUID());

      throw new RuntimeException(errorMessage);
    }
  }

  @Override
  public Set<MutationType> getSupportedRandomMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT, MutationType.UPDATE, MutationType.DELETE, MutationType.UPSERT);
  }

  @Override
  public Set<MutationType> getSupportedBulkMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT, MutationType.UPDATE, MutationType.DELETE, MutationType.UPSERT);
  }

  @Override
  public Iterable<Row> getExistingForFilters(Iterable<Row> filters) throws Exception {
    List<Row> existingForFilters = Lists.newArrayList();

    if (!filters.iterator().hasNext()) {
      return existingForFilters;
    }

    KuduTable table = getConnection().getTable(config.getString(TABLE_CONFIG_NAME));
    KuduScanner scanner = scannerForFilters(filters, table);

    long startTime = System.nanoTime();
    while (scanner.hasMoreRows()) {
      for (RowResult rowResult : scanner.nextRows()) {
        Row existing = resultAsRow(rowResult, table);

        existingForFilters.add(existing);
      }
    }
    long endTime = System.nanoTime();
    if (hasAccumulators()) {
      accumulators.getDoubleAccumulators().get(ACCUMULATOR_SECONDS_SCANNING).add((endTime - startTime) / 1000.0 / 1000.0 / 1000.0);
    }

    return existingForFilters;
  }


  private Row resultAsRow(RowResult result, KuduTable table) throws KuduException {
    List<Object> values = Lists.newArrayList();

    for (ColumnSchema columnSchema : table.getSchema().getColumns()) {
      String columnName = columnSchema.getName();

      if (result.isNull(columnName)) {
        values.add(null);
        continue;
      }

      switch (columnSchema.getType()) {
        case DOUBLE:
          values.add(result.getDouble(columnName));
          break;
        case FLOAT:
          values.add(result.getFloat(columnName));
          break;
        case INT8:
          values.add(result.getByte(columnName));
          break;
        case INT16:
          values.add(result.getShort(columnName));
          break;
        case INT32:
          values.add(result.getInt(columnName));
          break;
        case INT64:
          values.add(result.getLong(columnName));
          break;
        case STRING:
          values.add(result.getString(columnName));
          break;
        case BOOL:
          values.add(result.getBoolean(columnName));
          break;
        case BINARY:
          values.add(result.getBinaryCopy(columnName));
          break;
        case UNIXTIME_MICROS:
          values.add(KuduRelation.microsToTimestamp(result.getLong(columnName)));
          break;
        default:
          throw new RuntimeException("Unsupported Kudu column type: " + columnSchema.getType());
      }
    }

    Row row = new RowWithSchema(getTableSchema(table), values.toArray());

    return row;
  }

  private StructType schemaFor(KuduTable table) {
    List<String> fieldNames = Lists.newArrayList();
    List<String> fieldTypes = Lists.newArrayList();

    for (ColumnSchema columnSchema : table.getSchema().getColumns()) {
      String fieldName = columnSchema.getName();
      String fieldType;

      switch (columnSchema.getType()) {
        case DOUBLE:
          fieldType = "double";
          break;
        case FLOAT:
          fieldType = "float";
          break;
        case INT8:
          fieldType = "byte";
          break;
        case INT16:
          fieldType = "short";
          break;
        case INT32:
          fieldType = "int";
          break;
        case INT64:
          fieldType = "long";
          break;
        case STRING:
          fieldType = "string";
          break;
        case BOOL:
          fieldType = "boolean";
          break;
        case BINARY:
          fieldType = "binary";
          break;
        case UNIXTIME_MICROS:
          fieldType = "timestamp";
          break;
        default:
          throw new RuntimeException("Unsupported Kudu column type: " + columnSchema.getType());
      }

      fieldNames.add(fieldName);
      fieldTypes.add(fieldType);
    }

    StructType tableSchema = RowUtils.structTypeFor(fieldNames, fieldTypes);

    return tableSchema;
  }

  private KuduScanner scannerForFilters(Iterable<Row> filters, KuduTable table) throws KuduException {
    List<Row> filtersList = Lists.newArrayList(filters);

    if (filtersList.size() == 0) {
      throw new RuntimeException("Kudu existing filter was not provided.");
    }
    
    if (filtersList.get(0).schema() == null) {
      throw new RuntimeException("Kudu existing filter did not contain a schema.");
    }
    
    if (hasAccumulators()) {
      accumulators.getLongAccumulators().get(ACCUMULATOR_NUMBER_OF_SCANNERS).add(1);
      accumulators.getLongAccumulators().get(ACCUMULATOR_NUMBER_OF_FILTERS_SCANNED).add(filtersList.size());
    }
    
    KuduScannerBuilder builder = getConnection().getClient().newScannerBuilder(table);

    for (String fieldName : filtersList.get(0).schema().fieldNames()) {
      ColumnSchema columnSchema = table.getSchema().getColumn(fieldName);

      List<Object> columnValues = Lists.newArrayList();
      for (Row filter : filtersList) {
        Object columnValue = RowUtils.get(filter, fieldName);
        columnValues.add(columnValue);
      }

      KuduPredicate predicate = KuduPredicate.newInListPredicate(columnSchema, columnValues);

      builder = builder.addPredicate(predicate);
    }

    KuduScanner scanner = builder.build();

    return scanner;
  }

  private List<Operation> extractOperations(List<Row> planned, KuduTable table) throws Exception {
    List<Operation> operations = Lists.newArrayList();

    for (Row plan : planned) {
      MutationType mutationType = PlannerUtils.getMutationType(plan);

      Operation operation = null;

      switch (mutationType) {
        case DELETE:
          operation = table.newDelete();
          break;
        case INSERT:
          operation = table.newInsert();
          break;
        case UPDATE:
          operation = table.newUpdate();
          break;
        case UPSERT:
          operation = table.newUpsert();
          break;
        default:
          throw new RuntimeException("Unsupported Kudu mutation type: " + mutationType.toString());
      }

      PartialRow kuduRow = operation.getRow();

      if (plan.schema() == null) {
        throw new RuntimeException("Plan sent to Kudu output does not contain a schema");
      }
      
      plan = PlannerUtils.removeMutationTypeField(plan);

      List<String> kuduFieldNames = null;

      for (StructField field : plan.schema().fields()) {
        String fieldName = field.name();

        if (KuduUtils.ignoreMissingColumns(config)) {
          if (kuduFieldNames == null) {
            kuduFieldNames = Lists.newArrayList();
            for (ColumnSchema columnSchema : table.getSchema().getColumns()) {
              kuduFieldNames.add(columnSchema.getName());
            }
          }
          if (!kuduFieldNames.contains(fieldName)) {
            continue;
          }
        }

        ColumnSchema columnSchema = table.getSchema().getColumn(fieldName);

        if (!plan.isNullAt(plan.fieldIndex(fieldName))) {
          int fieldIndex = plan.fieldIndex(fieldName);
          switch (columnSchema.getType()) {
            case DOUBLE:
              kuduRow.addDouble(fieldName, plan.getDouble(fieldIndex));
              break;
            case FLOAT:
              kuduRow.addFloat(fieldName, plan.getFloat(fieldIndex));
              break;
            case INT8:
              kuduRow.addByte(fieldName, plan.getByte(fieldIndex));
              break;
            case INT16:
              kuduRow.addShort(fieldName, plan.getShort(fieldIndex));
              break;
            case INT32:
              kuduRow.addInt(fieldName, plan.getInt(fieldIndex));
              break;
            case INT64:
              kuduRow.addLong(fieldName, plan.getLong(fieldIndex));
              break;
            case STRING:
              kuduRow.addString(fieldName, plan.getString(fieldIndex));
              break;
            case BOOL:
              kuduRow.addBoolean(fieldName, plan.getBoolean(fieldIndex));
              break;
            case BINARY:
              kuduRow.addBinary(fieldName, plan.<byte[]>getAs(fieldIndex));
              break;
            case UNIXTIME_MICROS:
              kuduRow.addLong(fieldName, KuduRelation.timestampToMicros(plan.getTimestamp(fieldIndex)));
              break;
            default:
              throw new RuntimeException("Unsupported Kudu column type: " + columnSchema.getType());
          }
        }
      }

      operations.add(operation);
    }

    return operations;
  }

  private synchronized StructType getTableSchema(KuduTable table) throws KuduException {
    if (tableSchemas == null) {
      tableSchemas = Maps.newHashMap();
    }

    if (tableSchemas.containsKey(table.getName())) {
      return tableSchemas.get(table.getName());
    }
    else {
      StructType tableSchema = schemaFor(table);
      tableSchemas.put(table.getName(), tableSchema);
      return tableSchema;
    }
  }

  @Override
  public void applyBulkMutations(List<Tuple2<MutationType, Dataset<Row>>> planned) {
    KuduContext kc = new KuduContext(
        config.getString(CONNECTION_CONFIG_NAME), Contexts.getSparkSession().sparkContext());

    String tableName = config.getString(TABLE_CONFIG_NAME);

    Set<String> kuduColumns = null;
    if (KuduUtils.ignoreMissingColumns(config)) {
        try {
          KuduTable table = getConnection().getTable(tableName);
          kuduColumns = Sets.newHashSetWithExpectedSize(table.getSchema().getColumns().size());
          for (int i = 0; i < table.getSchema().getColumns().size(); i++) {
            ColumnSchema columnSchema = table.getSchema().getColumns().get(i);
            kuduColumns.add(columnSchema.getName());
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
    }

    for (Tuple2<MutationType, Dataset<Row>> plan : planned) {
      MutationType mutationType = plan._1();
      Dataset<Row> mutation = plan._2();

      if (KuduUtils.ignoreMissingColumns(config) && kuduColumns != null) {
        Set<String> mutationFields = Sets.newHashSet(mutation.schema().fieldNames());
        for (String col : Sets.difference(mutationFields, kuduColumns)) {
          mutation = mutation.drop(col);
        }
      }

      switch (mutationType) {
        case DELETE:
          kc.deleteRows(mutation, tableName);
          break;
        case INSERT:
          if (KuduUtils.doesInsertIgnoreDuplicates(config)) {
            kc.insertIgnoreRows(mutation, tableName);
          } else {
            kc.insertRows(mutation, tableName);
          }
          break;
        case UPDATE:
          kc.updateRows(mutation, tableName);
          break;
        case UPSERT:
          kc.upsertRows(mutation, tableName);
          break;
        default:
          throw new RuntimeException("Kudu bulk output does not support mutation type: " + mutationType);
      }
    }
  }

  /**
   * Returns whether or not we should ignore duplicate rows
   */
  private boolean isInsertIgnore() {
    return ConfigUtils.getOrElse(config, INSERT_IGNORE_CONFIG_NAME, false);
  }

  private KuduConnection getConnection() throws KuduException {
    return KuduConnectionManager.getKuduConnectionManager(config).getConnection();
  }

  /**
   * Returns whether or not we should ignore missing columns when writing to Kudu
   */
  private boolean ignoreMissingColumns() {
    return ConfigUtils.getOrElse(config, IGNORE_MISSING_COLUMNS_CONFIG_NAME, false);
  }
  
  private boolean hasAccumulators() {
    return accumulators != null;
  }

  @Override
  public Set<AccumulatorRequest> getAccumulatorRequests() {
    LOG.info("Kudu output requesting accumulators");
    
    return Sets.newHashSet(new AccumulatorRequest(ACCUMULATOR_NUMBER_OF_SCANNERS, Long.class),
                           new AccumulatorRequest(ACCUMULATOR_NUMBER_OF_FILTERS_SCANNED, Long.class),
                           new AccumulatorRequest(ACCUMULATOR_SECONDS_SCANNING, Double.class));
  }

  @Override
  public void receiveAccumulators(Accumulators accumulators) {
    this.accumulators = accumulators;
    
    LOG.info("Kudu output received accumulators");
  }

  @Override
  public String getAlias() {
    return "kudu";
  }
  
  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(CONNECTION_CONFIG_NAME, ConfigValueType.STRING)
        .mandatoryPath(TABLE_CONFIG_NAME, ConfigValueType.STRING)
        .optionalPath(INSERT_IGNORE_CONFIG_NAME, ConfigValueType.BOOLEAN)
        .optionalPath(IGNORE_MISSING_COLUMNS_CONFIG_NAME, ConfigValueType.BOOLEAN)
        .optionalPath(IS_SECURE_CONFIG_NAME, ConfigValueType.BOOLEAN)
        .addAll(SecurityUtils.getValidations())
        .build();
  }

}