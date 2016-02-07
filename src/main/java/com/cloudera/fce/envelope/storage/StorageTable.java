package com.cloudera.fce.envelope.storage;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.fce.envelope.RecordModel;
import com.cloudera.fce.envelope.planner.PlannedRecord;
import com.cloudera.fce.envelope.utils.PropertiesUtils;

public abstract class StorageTable {
    
    protected Properties props;
    protected String tableName;
    private RecordModel recordModel = new RecordModel();
    
    public StorageTable(Properties props) {
        this.props = props;
        
        this.tableName = props.getProperty("table.name");
        
        recordModel.setKeyFieldNames(PropertiesUtils.propertyAsList(props, "table.columns.key"));
        recordModel.setTimestampFieldName(props.getProperty("table.column.timestamp"));
        recordModel.setValueFieldNames(PropertiesUtils.propertyAsList(props, "table.columns.values"));
        recordModel.setLastUpdatedFieldName(props.getProperty("table.column.last.updated"));
        recordModel.setEffectiveFromFieldName(props.getProperty("table.column.effective.from"));
        recordModel.setEffectiveToFieldName(props.getProperty("table.column.effective.to"));
        recordModel.setCurrentFlagFieldName(props.getProperty("table.column.current.flag"));
    }
    
    public abstract void connect() throws Exception;
    public abstract void disconnect() throws Exception;
    
    public abstract Schema getSchema();
    public RecordModel getRecordModel() {
        return recordModel;
    }
    
    public abstract List<GenericRecord> getExistingForArriving(List<GenericRecord> arriving) throws Exception;
    
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
