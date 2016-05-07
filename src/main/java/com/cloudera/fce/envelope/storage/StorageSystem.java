package com.cloudera.fce.envelope.storage;

import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Maps;

public abstract class StorageSystem {
    
    protected Properties props;
    private Map<String, StorageTable> tables = Maps.newHashMap();
    
    public StorageSystem(Properties props) {
        this.props = props;
    }
    
    public StorageTable getStorageTable(Properties props) throws Exception {
        String tableName = props.getProperty("storage.table.name");
        
        if (!tables.containsKey(tableName)) {
            StorageTable table = tableFor(props);
            tables.put(tableName, table);
        }
        
        return tables.get(tableName);
    }
    
    public abstract void connect() throws Exception;
    public abstract void disconnect() throws Exception;
    
    public abstract StorageTable tableFor(Properties props) throws Exception;
    
}
