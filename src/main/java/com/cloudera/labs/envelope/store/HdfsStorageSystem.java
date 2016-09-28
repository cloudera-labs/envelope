package com.cloudera.labs.envelope.store;

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single HDFS FileSystem connection within the JVM
 */
public class HdfsStorageSystem extends StorageSystem {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsStorageSystem.class);

  private FileSystem fileSystem;

  public HdfsStorageSystem(Properties properties) {
    super(properties);
  }

  /**
   * Open a connection to the storage system.
   */
  @Override
  public void connect() throws Exception {
    this.fileSystem = FileSystem.newInstance(new Configuration());
  }

  /**
   * Close the connection to the storage system.
   */
  @Override
  public void disconnect() throws Exception {
    this.fileSystem.close();
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
    return new HdfsStorageTable(this.fileSystem, this.props);
  }
}
