package com.cloudera.fce.envelope.store;

import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Maps;

/**
 * An instance of a storage system, as defined by a connection to it. Maintains a handle
 * to requested tables within the system so that they can be reused by future tasks.
 */
public abstract class StorageSystem {
    
    /**
     * The properties of the storage for the flow.
     */
    protected Properties props;
    
    /**
     * The tables for the storage system instance, as identified by their table names.
     */
    private Map<String, StorageTable> tables = Maps.newHashMap();
    
    public StorageSystem(Properties props) {
        this.props = props;
    }
    
    /**
     * Get a storage table object for the given properties. If the storage table has been
     * previously requested in this JVM then it will be reused.
     * @param props The properties of the storage table.
     * @return The storage table.
     */
    StorageTable getStorageTable(Properties props) throws Exception {
        String tableName = props.getProperty("storage.table.name");
        
        if (!tables.containsKey(tableName)) {
            StorageTable table = tableFor(props);
            tables.put(tableName, table);
        }
        
        return tables.get(tableName);
    }
    
    /**
     * Open a connection to the storage system.
     */
    public abstract void connect() throws Exception;
    
    /**
     * Close the connection to the storage system.
     */
    public abstract void disconnect() throws Exception;
    
    /**
     * Instantiate a storage table object for the given properties.
     * @param props The properties of the storage for the flow.
     * @return The storage table for the flow.
     * @throws Exception
     */
    public abstract StorageTable tableFor(Properties props) throws Exception;
    
}
