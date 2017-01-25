package com.cloudera.labs.envelope.output;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;
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
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

public class KuduOutput implements RandomOutput, BulkOutput {
    
    public static final String CONNECTION_CONFIG_NAME = "connection";
    public static final String TABLE_CONFIG_NAME = "table.name"; 
    
    private Config config;
    
    private static KuduClient client;
    private static KuduSession session;
    private static Map<String, KuduTable> tables;
    private static Map<String, StructType> tableSchemas;
    
    private static Logger LOG = LoggerFactory.getLogger(KuduOutput.class);

    @Override
    public void configure(Config config) {
        this.config = config;
    }

    @Override
    public void applyRandomMutations(List<PlannedRow> planned) throws Exception {
        KuduTable table = connectToTable();
        
        List<Operation> operations = extractOperations(planned, table);
        
        for (Operation operation : operations) {
            session.apply(operation);
        }
        
        // Wait until all operations have completed before checking for errors.
        while (session.hasPendingOperations()) {
            Thread.sleep(1);
        }
        
        // Log Kudu operation errors instead of stopping the job.
        if (session.countPendingErrors() > 0) {
            RowError[] errors = session.getPendingErrors().getRowErrors();
            
            for (RowError error : errors) {
                LOG.error("Error '{}' during operation '{}' at tablet server '{}'", error.getErrorStatus().toString(), error.getOperation(), error.getTsUUID());
            }
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
                
        KuduTable table = connectToTable();
        KuduScanner scanner = scannerForFilters(filters, table);
        
        while (scanner.hasMoreRows()) {
            for (RowResult rowResult : scanner.nextRows()) {
                Row existing = resultAsRow(rowResult, table);
                
                existingForFilters.add(existing);
            }
        }
        
        return existingForFilters;
    }
    
    private synchronized KuduTable connectToTable() throws KuduException {
        if (client == null) {
            LOG.info("Connecting to Kudu");
            
            String masterAddresses = config.getString(CONNECTION_CONFIG_NAME);
            
            client = new KuduClient.KuduClientBuilder(masterAddresses).build();
            session = client.newSession();
            session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
            session.setMutationBufferSpace(10000);
            
            LOG.info("Connection to Kudu established");
        }
        
        String tableName = config.getString(TABLE_CONFIG_NAME);
        KuduTable table = getTable(tableName);
        
        return table;
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
                default:
                    throw new RuntimeException("Unsupported Kudu column type: " + columnSchema.getType());
            }
            
            fieldNames.add(fieldName);
            fieldTypes.add(fieldType);
        }
        
        StructType tableSchema = RowUtils.structTypeFor(fieldNames, fieldTypes);
        
        return tableSchema;
    }
    
    private KuduScanner scannerForFilters(Iterable<Row> filters, KuduTable table) {
        List<Row> filtersList = Lists.newArrayList(filters);
        
        if (filtersList.size() == 0) {
            throw new RuntimeException("Kudu existing filter was not provided.");
        }
        
        KuduScannerBuilder builder = client.newScannerBuilder(table);
        
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
    
    private List<Operation> extractOperations(List<PlannedRow> planned, KuduTable table) throws Exception {
        List<Operation> operations = Lists.newArrayList();
        
        for (PlannedRow plan : planned) {
            MutationType mutationType = plan.getMutationType();
            
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
            Row planRow = plan.getRow();
            
            if (planRow.schema() == null) {
                throw new RuntimeException("Plan sent to Kudu output does not contain a schema");
            }
            
            for (StructField field : planRow.schema().fields()) {
                String fieldName = field.name();
                ColumnSchema columnSchema = table.getSchema().getColumn(fieldName);
                
                if (!planRow.isNullAt(planRow.fieldIndex(fieldName))) {
                    int fieldIndex = planRow.fieldIndex(fieldName);
                    switch (columnSchema.getType()) {
                        case DOUBLE:
                            kuduRow.addDouble(fieldName, planRow.getDouble(fieldIndex));
                            break;
                        case FLOAT:
                            kuduRow.addFloat(fieldName, planRow.getFloat(fieldIndex));
                            break;
                        case INT32:
                            kuduRow.addInt(fieldName, planRow.getInt(fieldIndex));
                            break;
                        case INT64:
                            kuduRow.addLong(fieldName, planRow.getLong(fieldIndex));
                            break;
                        case STRING:
                            kuduRow.addString(fieldName, planRow.getString(fieldIndex));
                            break;
                        case BOOL:
                            kuduRow.addBoolean(fieldName, planRow.getBoolean(fieldIndex));
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
    
    private synchronized KuduTable getTable(String tableName) throws KuduException {
        if (tables == null) {
            tables = Maps.newHashMap();
        }
        
        if (tables.containsKey(tableName)) {
            return tables.get(tableName);
        }
        else {
            KuduTable table = client.openTable(tableName);
            tables.put(tableName, table);
            return table;
        }
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
    public void applyBulkMutations(List<Tuple2<MutationType, DataFrame>> planned) throws Exception {
        KuduContext kc = new KuduContext(config.getString(CONNECTION_CONFIG_NAME));
        
        for (Tuple2<MutationType, DataFrame> plan : planned) {
            MutationType mutationType = plan._1();
            DataFrame mutation = plan._2();
            String tableName = config.getString(TABLE_CONFIG_NAME);
            
            switch (mutationType) {
                case DELETE:
                    kc.deleteRows(mutation, tableName);
                    break;
                case INSERT:
                    kc.insertRows(mutation, tableName);
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

}
