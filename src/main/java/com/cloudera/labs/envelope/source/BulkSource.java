package com.cloudera.labs.envelope.source;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class BulkSource implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BulkSource.class);

  protected Properties props;

  public BulkSource(Properties props) {
    this.props = props;
  }

  public Schema getSchema() {
    return null;
  }

  /**
   *
   * @param jsc
   * @param props
   * @return
   */
  public abstract JavaRDD<GenericRecord> rddFor(JavaSparkContext jsc, Properties props);

  /**
   * The queue source for the application.
   * @param props The properties of the queue source.
   * @return The queue source.
   */
  public static BulkSource bulkSourceFor(Properties props) throws Exception {
    BulkSource source = null;

    String sourceName = props.getProperty("source");

    if (sourceName.equals("hdfs")) {
      source = new HdfsBulkSource(props);
    }
    else {
      Class<?> clazz = Class.forName(sourceName);
      Constructor<?> constructor = clazz.getConstructor(Properties.class);
      source = (BulkSource)constructor.newInstance(props);
    }

    return source;
  }

}
