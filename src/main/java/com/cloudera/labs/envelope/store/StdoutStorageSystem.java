package com.cloudera.labs.envelope.store;

import java.util.Properties;

/**
 *
 */
public class StdoutStorageSystem extends StorageSystem {

  public StdoutStorageSystem(Properties properties) {
    super(properties);
  }

  /**
   * Open a connection to the storage system.
   */
  @Override
  public void connect() throws Exception {
    // no op
  }

  /**
   * Close the connection to the storage system.
   */
  @Override
  public void disconnect() throws Exception {
    // no op
  }

  /**
   * Instantiate a storage table object for the given properties.
   *
   * @param props The properties of the storage for the flow.
   * @return The storage table for the flow.
   * @throws Exception
   */
  @Override
  public StorageTable tableFor(Properties props) throws Exception {
    return new StdoutStorageTable(props.getProperty("storage.table.name"));
  }
}
