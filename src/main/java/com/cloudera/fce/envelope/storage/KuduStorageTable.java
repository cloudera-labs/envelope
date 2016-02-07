package com.cloudera.fce.envelope.storage;

import java.util.List;
import java.util.Properties;
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
import org.kududb.client.SessionConfiguration.FlushMode;
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
    
    public KuduStorageTable(Properties props) {
        super(props);
    }
    
    @Override
    public void connect() throws Exception {
        String masterAddresses = props.getProperty("kudu.connection");
        client = new KuduClient.KuduClientBuilder(masterAddresses).build();
        session = client.newSession();
        table = client.openTable(tableName);
        
        // We don't want to silently drop duplicates because there shouldn't be any.
        session.setIgnoreAllDuplicateRows(false);
        // Tell the Kudu client that we will control when we want it to flush operations.
        // Without this we would flush individual operations and throughput would plummet.
        session.setFlushMode(FlushMode.MANUAL_FLUSH);
    }
    
    @Override
    public void disconnect() throws Exception {
        session.close();
        client.shutdown();
    }
    
    @Override
    public void applyPlannedOperations(List<PlannedRecord> planned) throws Exception {
        List<Operation> operations = extractOperations(planned);
        
        int count = 0;
        
        for (Operation operation : operations) {
            session.apply(operation);
            
            // Flush every 500 operations.
            // TODO: optimize this figure
            if (count++ == 500) {
                session.flush();
                count = 0;
                
                // TODO: capture errors
            }            
        }
        
        session.flush();        
    }
    
    // TODO: allow for deletes
    private List<Operation> extractOperations(List<PlannedRecord> planned) throws Exception {
        List<Operation> operations = Lists.newArrayList();
        
        for (PlannedRecord plan : planned) {
            OperationType operationType = plan.getOperationType();
            
            // There are no operations if the record already existed and was not modified.
            if (!operationType.equals(OperationType.NONE)) {
                Operation operation = null;
                
                // Kudu requires us to treat inserts and updates separately.
                if (operationType.equals(OperationType.INSERT)) {
                    operation = table.newInsert();
                }
                else if (operationType.equals(OperationType.UPDATE)) {
                    operation = table.newUpdate();
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
                                kuduRow.addString(outputColName, (String)plan.get(outputColName));
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
    
    @Override
    public List<GenericRecord> getExistingForArriving(List<GenericRecord> arriving) throws Exception {
        List<String> keyFieldNames = getRecordModel().getKeyFieldNames();
        
        Set<GenericRecord> arrivingKeys = Sets.newHashSet();
        for (GenericRecord arrived : arriving) {
            arrivingKeys.add(RecordUtils.subsetRecord(arrived, keyFieldNames));
        }
        
        List<GenericRecord> existing = Lists.newArrayList();
        
        for (GenericRecord key : arrivingKeys) {
            KuduScanner scanner = scannerForKey(key);
            while (scanner.hasMoreRows()) {
                for (RowResult rowResult : scanner.nextRows()) {
                    existing.add(resultAsRecord(rowResult));
                }
            }
        }
        
        return existing;
    }
    
    @Override
    public Schema getSchema() throws RuntimeException {
        // Use the cached schema if we have already created it.
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
                default:
                    throw new RuntimeException("Unsupported Kudu column type: " + columnSchema.getType());
            }
            
            fieldNames.add(fieldName);
            fieldTypes.add(fieldType);
        }
        
        tableSchema = RecordUtils.schemaFor(fieldNames, fieldTypes);
        
        return tableSchema;
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
                default:
                    throw new RuntimeException("Unsupported Kudu column type: " + columnSchema.getType());
            }
        }
        
        return record;
    }
    
    // The Kudu scanner that corresponds to the provided key for the provided table.
    private KuduScanner scannerForKey(GenericRecord key) {
        KuduScannerBuilder builder = client.newScannerBuilder(table);
        
        // Build the Kudu scanner one key field at a time.
        for (Field field : key.getSchema().getFields()) {
            String columnName = field.name();
            Object columnValue = key.get(columnName);
            ColumnRangePredicate predKey = new ColumnRangePredicate(table.getSchema().getColumn(columnName));
            ColumnSchema columnSchema = table.getSchema().getColumn(columnName);
            
            switch (columnSchema.getType()) {
                case DOUBLE:
                    predKey.setLowerBound((Double)columnValue);
                    predKey.setUpperBound((Double)columnValue);
                    break;
                case FLOAT:
                    predKey.setLowerBound((Float)columnValue);
                    predKey.setUpperBound((Float)columnValue);
                    break;
                case INT32:
                    predKey.setLowerBound((Integer)columnValue);
                    predKey.setUpperBound((Integer)columnValue);
                    break;
                case INT64:
                    predKey.setLowerBound((Long)columnValue);
                    predKey.setUpperBound((Long)columnValue);
                    break;
                case STRING:
                    predKey.setLowerBound((String)columnValue);
                    predKey.setUpperBound((String)columnValue);
                    break;
                default:
                    throw new RuntimeException("Unsupported Kudu column type: " + columnSchema.getType());
            }
            
            builder = builder.addColumnRangePredicate(predKey);
        }
        
        return builder.build();
    }

}
