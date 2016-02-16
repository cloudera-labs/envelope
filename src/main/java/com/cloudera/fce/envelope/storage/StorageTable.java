package com.cloudera.fce.envelope.storage;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.fce.envelope.RecordModel;
import com.cloudera.fce.envelope.planner.OperationType;
import com.cloudera.fce.envelope.planner.PlannedRecord;
import com.cloudera.fce.envelope.utils.PropertiesUtils;

public abstract class StorageTable {
    
    protected Properties props;
    protected String tableName;
    
    public StorageTable(Properties props) {
        this.props = props;
        this.tableName = props.getProperty("table.name");
    }
    
    public abstract void connect() throws Exception;
    public abstract void disconnect() throws Exception;
    
    public abstract Set<OperationType> getSupportedOperationTypes();
    public abstract Schema getSchema();
    
    public abstract List<GenericRecord> getExistingForArriving(List<GenericRecord> arriving, RecordModel recordModel) throws Exception;
    
    public abstract void applyPlannedOperations(List<PlannedRecord> operations) throws Exception;
    
    public static StorageTable storageTableFor(Properties props) throws Exception {
        Properties storageProps = PropertiesUtils.prefixProperties(props, "storage.");
        String storageTableName = props.getProperty("storage");
        
        StorageTable storage = null;
        
        if (storageTableName.equals("kudu")) {
            storage = new KuduStorageTable(storageProps);
        }
        else {
            Class<?> clazz = Class.forName(storageTableName);
            Constructor<?> constructor = clazz.getConstructor();
            storage = (StorageTable)constructor.newInstance(storageProps);
        }
        
        return storage;
    }
    
}
