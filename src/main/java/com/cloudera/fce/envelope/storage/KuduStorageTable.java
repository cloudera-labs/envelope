package com.cloudera.fce.envelope.storage;

import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.kududb.ColumnSchema;
import org.kududb.client.ColumnRangePredicate;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduScanner.KuduScannerBuilder;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.Operation;
import org.kududb.client.PartialRow;
import org.kududb.client.RowResult;
import org.kududb.client.shaded.com.google.common.collect.Sets;

import com.cloudera.fce.envelope.planner.OperationType;
import com.cloudera.fce.envelope.planner.PlannedRecord;
import com.cloudera.fce.envelope.utils.RecordUtils;
import com.google.common.collect.Lists;

public class KuduStorageTable extends StorageTable {
    
    private KuduClient client;
    private KuduSession session;
    private KuduTable table;
    private Schema tableSchema;
    
    public KuduStorageTable(KuduStorageSystem system, String tableName) throws Exception {
        this.client = system.getClient();
        this.session = system.getSession();
        this.table = system.getClient().openTable(tableName);
    }
    
    @Override
    public void applyPlannedOperations(List<PlannedRecord> planned) throws Exception {
        List<Operation> operations = extractOperations(planned);
        
        int count = 0;
        
        for (Operation operation : operations) {
            session.apply(operation);
            
            // Flush every 500 operations.
            if (count++ == 500) {
                session.flush();
                count = 0;
                
                // TODO: capture errors
            }            
        }
        
        session.flush();        
    }
    
    @Override
    public List<GenericRecord> getExistingForFilter(GenericRecord filter) throws Exception {
        List<GenericRecord> existing = Lists.newArrayList();
        
        KuduScanner scanner = scannerForFilter(filter);
        while (scanner.hasMoreRows()) {
            for (RowResult rowResult : scanner.nextRows()) {
                existing.add(resultAsRecord(rowResult));
            }
        }
        
        return existing;
    }
    
    @Override
    public Schema getSchema() throws RuntimeException {
        if (tableSchema != null) {
            return tableSchema;
        }
        
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
        
        tableSchema = RecordUtils.schemaFor(fieldNames, fieldTypes);
        
        return tableSchema;
    }
    
    @Override
    public Set<OperationType> getSupportedOperationTypes() {
        return Sets.newHashSet(OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE);
    }
    
    private List<Operation> extractOperations(List<PlannedRecord> planned) throws Exception {
        List<Operation> operations = Lists.newArrayList();
        
        for (PlannedRecord plan : planned) {
            OperationType operationType = plan.getOperationType();
            
            if (!operationType.equals(OperationType.NONE)) {
                Operation operation = null;
                
                switch (operationType) {
                    case DELETE:
                        operation = table.newDelete();
                        break;
                    case INSERT:
                        operation = table.newInsert();
                        break;
                    case UPDATE:
                        operation = table.newUpdate();
                        break;
                    default:
                        throw new RuntimeException("Unknown operation type: " + operationType.toString());
                }
                
                PartialRow kuduRow = operation.getRow();
                
                for (ColumnSchema columnSchema : table.getSchema().getColumns()) {
                    String outputColName = columnSchema.getName();
                    
                    if (plan.get(outputColName) != null) {
                        switch (columnSchema.getType()) {
                            case DOUBLE:
                                kuduRow.addDouble(outputColName, (Double)plan.get(outputColName));
                                break;
                            case FLOAT:
                                kuduRow.addFloat(outputColName, (Float)plan.get(outputColName));
                                break;
                            case INT32:
                                kuduRow.addInt(outputColName, (Integer)plan.get(outputColName));
                                break;
                            case INT64:
                                kuduRow.addLong(outputColName, (Long)plan.get(outputColName));
                                break;
                            case STRING:
                                kuduRow.addString(outputColName, plan.get(outputColName).toString());
                                break;
                            case BOOL:
                                kuduRow.addBoolean(outputColName, (Boolean)plan.get(outputColName));
                                break;
                            default:
                                throw new RuntimeException("Unsupported Kudu column type: " + columnSchema.getType());
                        }
                    }
                }
                
                operations.add(operation);
            }
        }
        
        return operations;
    }
    
    // The Avro record representation of the Kudu scan result.
    private GenericRecord resultAsRecord(RowResult result) {
        GenericRecord record = new GenericData.Record(getSchema());
        
        for (ColumnSchema columnSchema : table.getSchema().getColumns()) {
            String columnName = columnSchema.getName();
            
            if (result.isNull(columnName)) {
                break;
            }
            
            switch (columnSchema.getType()) {
                case DOUBLE:
                    record.put(columnName, result.getDouble(columnName));
                    break;
                case FLOAT:
                    record.put(columnName, result.getFloat(columnName));
                    break;
                case INT32:
                    record.put(columnName, result.getInt(columnName));
                    break;
                case INT64:
                    record.put(columnName, result.getLong(columnName));
                    break;
                case STRING:
                    record.put(columnName, result.getString(columnName));
                    break;
                case BOOL:
                    record.put(columnName, result.getBoolean(columnName));
                    break;
                default:
                    throw new RuntimeException("Unsupported Kudu column type: " + columnSchema.getType());
            }
        }
        
        return record;
    }
    
    private KuduScanner scannerForFilter(GenericRecord filter) {
        KuduScannerBuilder builder = client.newScannerBuilder(table);
        
        for (Field field : filter.getSchema().getFields()) {
            String columnName = field.name();
            Object columnValue = filter.get(columnName);
            ColumnRangePredicate predicate = new ColumnRangePredicate(table.getSchema().getColumn(columnName));
            ColumnSchema columnSchema = table.getSchema().getColumn(columnName);
            
            switch (columnSchema.getType()) {
                case DOUBLE:
                    predicate.setLowerBound((Double)columnValue);
                    predicate.setUpperBound((Double)columnValue);
                    break;
                case FLOAT:
                    predicate.setLowerBound((Float)columnValue);
                    predicate.setUpperBound((Float)columnValue);
                    break;
                case INT32:
                    predicate.setLowerBound((Integer)columnValue);
                    predicate.setUpperBound((Integer)columnValue);
                    break;
                case INT64:
                    predicate.setLowerBound((Long)columnValue);
                    predicate.setUpperBound((Long)columnValue);
                    break;
                case STRING:
                    predicate.setLowerBound(columnValue.toString());
                    predicate.setUpperBound(columnValue.toString());
                    break;
                case BOOL:
                    predicate.setLowerBound((Boolean)columnValue);
                    predicate.setUpperBound((Boolean)columnValue);
                    break;
                default:
                    throw new RuntimeException("Unsupported Kudu column type: " + columnSchema.getType());
            }
            
            builder = builder.addColumnRangePredicate(predicate);
        }
        
        return builder.build();
    }
    
}
