package com.cloudera.fce.nrtkudu;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.kududb.ColumnSchema;
import org.kududb.client.ColumnRangePredicate;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.Operation;
import org.kududb.client.PartialRow;
import org.kududb.client.RowResult;
import org.kududb.client.KuduScanner.KuduScannerBuilder;
import org.kududb.client.SessionConfiguration.FlushMode;

import com.google.common.collect.Lists;

// Base class for using Kudu as the encoder storage layer.
// Similar classes should be straight-forward to create for HBase, Cassandra, etc.

public abstract class KuduEncoder extends Encoder {
    
    // The Kudu connection objects.
    private KuduClient client;
    private KuduSession session;
    private KuduTable table;
    
    // Cache the Kudu output schema so that we only have to create it once per micro-batch partition.
    private Schema outputSchema;
  
    @Override
    protected void connect() throws Exception {
        client = new KuduClient.KuduClientBuilder(this.connection).build();
        table = client.openTable(getTableName());
        session = client.newSession();
        
        // We don't want to silently drop duplicates because there shouldn't be any.
        session.setIgnoreAllDuplicateRows(false);
        // Tell the Kudu client that we will control when we want it to flush operations.
        // Without this we would flush individual operations and throughput would plummet.
        session.setFlushMode(FlushMode.MANUAL_FLUSH);
    }
    
    @Override
    protected void disconnect() throws Exception {
        session.close();
        client.shutdown();
    }
    
    @Override
    protected List<GenericRecord> recordsFromOutputForKey(Map<String, Object> key) throws Exception {
        List<GenericRecord> outputsForKey = Lists.newArrayList();
        
        // Scan for the key and translate all existing records found to Avro.
        KuduScanner scanner = keyScanner(client, table, key);
        while (scanner.hasMoreRows()) {
            for (RowResult rowResult : scanner.nextRows()) {
                outputsForKey.add(resultAsKuduGenericRecord(rowResult));
            }
        }
        
        return outputsForKey;
    }
    
    @Override
    protected List<Object> extractOperations(List<GenericRecord> outputs) {
        List<Object> operations = Lists.newArrayList();
        
        for (GenericRecord output : outputs) {
            // There are no operations if the record already existed and was not modified.
            if (!output.get(OPERATION_FIELD_NAME).equals(OPERATION_NONE)) {
                Operation operation = null;
                
                // Kudu requires us to treat inserts and updates separately.
                if (output.get(OPERATION_FIELD_NAME).equals(OPERATION_INSERT)) {
                    operation = table.newInsert();
                }
                else if (output.get(OPERATION_FIELD_NAME).equals(OPERATION_UPDATE)) {
                    operation = table.newUpdate();
                }
                
                PartialRow row = operation.getRow();
                                
                for (ColumnSchema columSchema : table.getSchema().getColumns()) {
                    String outputColName = columSchema.getName();

                    if (output.get(outputColName) != null) {
                        switch (columSchema.getType()) {
                            case BINARY:
                                break;
                            case BOOL:
                                break;
                            case DOUBLE:
                                row.addDouble(outputColName, (Double)output.get(outputColName));
                                break;
                            case FLOAT:
                                row.addFloat(outputColName, (Float)output.get(outputColName));
                                break;
                            case INT16:
                                break;
                            case INT32:
                                row.addInt(outputColName, (Integer)output.get(outputColName));
                                break;
                            case INT64:
                                row.addLong(outputColName, (Long)output.get(outputColName));
                                break;
                            case INT8:
                                break;
                            case STRING:
                                row.addString(outputColName, (String)output.get(outputColName));
                                break;
                            case TIMESTAMP:
                                break;
                            default:
                                break;
                        }
                    }
                }
                
                operations.add(operation);
            }
        }
        
        return operations;
    }
    
    @Override
    protected void applyOperations(List<Object> operations) throws Exception {
        int count = 0;
        
        for (Object operationObject : operations) {
            Operation operation = (Operation)operationObject;
            
            // TODO: capture errors
            session.apply(operation);
            
            // Flush every 500 operations.
            if (count++ == 500) {
                session.flush();
                count = 0;
            }
        }
        
        session.flush();
    }
    
    @Override
    protected Schema outputSchema() {
        // Use the cached schema if we have already created it.
        if (outputSchema != null) {
            return outputSchema;
        }
        
        FieldAssembler<Schema> assembler = SchemaBuilder.record(getTableName()).fields();
        
        for (ColumnSchema columnSchema : table.getSchema().getColumns()) {
            switch (columnSchema.getType()) {
                case BINARY:
                    break;
                case BOOL:
                    break;
                case DOUBLE:
                    assembler = assembler.optionalDouble(columnSchema.getName());
                    break;
                case FLOAT:
                    assembler = assembler.optionalFloat(columnSchema.getName());
                    break;
                case INT16:
                    break;
                case INT32:
                    assembler = assembler.optionalInt(columnSchema.getName());
                    break;
                case INT64:
                    assembler = assembler.optionalLong(columnSchema.getName());
                    break;
                case INT8:
                    break;
                case STRING:
                    assembler = assembler.optionalString(columnSchema.getName());
                    break;
                case TIMESTAMP:
                    break;
                default:
                    break;
            }
        }
        
        assembler = assembler.optionalString(OPERATION_FIELD_NAME);
        
        // Cache the schema for future calls.
        outputSchema = assembler.endRecord();
        
        return outputSchema;
    }
    
    // The Avro record representation of the Kudu scan result.
    private GenericRecord resultAsKuduGenericRecord(RowResult result) {        
        GenericRecord record = new GenericData.Record(outputSchema());
        
        for (ColumnSchema columnSchema : table.getSchema().getColumns()) {
            String columnName = columnSchema.getName();
            
            if (result.isNull(columnName)) {
                record.put(columnName, null);
                break;
            }
            
            switch (columnSchema.getType()) {
                case BINARY:
                    break;
                case BOOL:
                    break;
                case DOUBLE:
                    record.put(columnName, result.getDouble(columnName));
                    break;
                case FLOAT:
                    record.put(columnName, result.getFloat(columnName));
                    break;
                case INT16:
                    break;
                case INT32:
                    record.put(columnName, result.getInt(columnName));
                    break;
                case INT64:
                    record.put(columnName, result.getLong(columnName));
                    break;
                case INT8:
                    break;
                case STRING:
                    record.put(columnName, result.getString(columnName));
                    break;
                case TIMESTAMP:
                    break;
                default:
                    break;
            }
        }
        
        record.put(OPERATION_FIELD_NAME, OPERATION_NONE);
        
        return record;
    }
    
    // The Kudu scanner that corresponds to the provided key for the provided table.
    private KuduScanner keyScanner(KuduClient client, KuduTable table, Map<String, Object> key) {
        KuduScannerBuilder builder = client.newScannerBuilder(table);
        
        // Build the Kudu scanner one key field at a time.
        for (Map.Entry<String, Object> entry : key.entrySet()) {
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();
            ColumnRangePredicate predKey = new ColumnRangePredicate(table.getSchema().getColumn(columnName));
            
            switch (table.getSchema().getColumn(columnName).getType()) {
                case BINARY:
                    break;
                case BOOL:
                    break;
                case DOUBLE:
                    predKey.setLowerBound((Double)columnValue);
                    predKey.setUpperBound((Double)columnValue);
                    break;
                case FLOAT:
                    predKey.setLowerBound((Float)columnValue);
                    predKey.setUpperBound((Float)columnValue);
                    break;
                case INT16:
                    break;
                case INT32:
                    predKey.setLowerBound((Integer)columnValue);
                    predKey.setUpperBound((Integer)columnValue);
                    break;
                case INT64:
                    predKey.setLowerBound((Long)columnValue);
                    predKey.setUpperBound((Long)columnValue);
                    break;
                case INT8:
                    break;
                case STRING:
                    predKey.setLowerBound((String)columnValue);
                    predKey.setUpperBound((String)columnValue);
                    break;
                case TIMESTAMP:
                    break;
                default:
                    break;
            }
            
            builder = builder.addColumnRangePredicate(predKey);
        }
        
        return builder.build();
    }
    
}
