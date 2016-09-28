package com.cloudera.labs.envelope;

import com.cloudera.labs.envelope.source.BulkSource;
import com.cloudera.labs.envelope.utils.PropertiesUtils;
import com.cloudera.labs.envelope.utils.SparkSQLAvroUtils;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures the Apache Spark batch job and runs it.
 */
@SuppressWarnings("serial")
public class Batcher {

  private static final Logger LOG = LoggerFactory.getLogger(Batcher.class);
  private static final String DEFAULT_TABLE_NAME = "batch";

  public static void main(String[] args) throws Exception {

    // Retrieve the properties for the pipeline being executed
    final Properties props = PropertiesUtils.loadProperties(args[0]);

    // Instantiate the Spark batch job
    JavaSparkContext jsc = getBatchContext(props);

    // Initialize the batch
    final BulkSource bs = BulkSource.bulkSourceFor(props);
    JavaRDD<GenericRecord> batch = bs.rddFor(jsc, props);

    // If required, repartition the batch across the executors
    if (doesRepartition(props)) {
      batch = repartition(batch, props);
    }

    // Initialize Spark SQL
    final SQLContext sqlc = new SQLContext(jsc.sc());

    // This is what we want to do for the batch

    DataFrame batchDataFrame = null;
    Map<String, DataFrame> lookupDataFrames = null;

    // Create the flows that we are going to run for the batch
    Set<Flow> flows = Flow.flowsFor(props);

    // We only use DataFrames in derivers, so we only need to convert to a DataFrame
    // if there is at least one deriver in the pipeline
    if (hasAtLeastOneDeriver(flows)) {
      // Convert the batch to a DataFrame
      Schema bsSchema = bs.getSchema();
      batchDataFrame = SparkSQLAvroUtils.dataFrameForRecords(batch, bsSchema, sqlc);
      batchDataFrame.registerTempTable(getTableName(props));
      batchDataFrame.persist(StorageLevel.MEMORY_ONLY());

      // Load all the required lookups for the batch
      lookupDataFrames = Maps.newHashMap();
      Set<Lookup> lookups = Lookup.lookupsFor(props);
      for (Lookup lookup : lookups) {
        JavaRDD<GenericRecord> lookupRecords = lookup.getLookupRecordsFor(batch);
        Schema lookupSchema = lookup.getLookupTableSchema();
        DataFrame lookupDataFrame = SparkSQLAvroUtils.dataFrameForRecords(lookupRecords, lookupSchema, sqlc);
        String lookupTableName = lookup.getLookupTableName();
        lookupDataFrame.registerTempTable(lookupTableName);
        lookupDataFrame.persist(StorageLevel.MEMORY_ONLY());
        lookupDataFrames.put(lookupTableName, lookupDataFrame);
      }
    }

    // Run each of the flows of the pipeline
    for (Flow flow : flows) {
      if (flow.hasDeriver()) {
        LOG.debug("Executing Flow[{}] with deriver", flow.toString());
        flow.runFlow(batchDataFrame, lookupDataFrames);
      }
      // Flows that don't have derivers do not need to pay the penalty of converting
      // into and back from DataFrames, so we just skip that part altogether
      else {
        LOG.debug("Executing Flow[{}]", flow.toString());
        flow.runFlow(batch);
      }
    }

    // Remove all the cached DataFrames from memory
    if (hasAtLeastOneDeriver(flows)) {
      batchDataFrame.unpersist();
      for (DataFrame lookupDataFrame : lookupDataFrames.values()) {
        lookupDataFrame.unpersist();
      }
    }

    jsc.close();
  }

  private static JavaSparkContext getBatchContext(final Properties props) {
    final SparkConf sparkConf = getSparkConfiguration(props);

    String applicationName = props.getProperty("application.name");
    sparkConf.setAppName(applicationName);

    return new JavaSparkContext(sparkConf);
  }

  private static SparkConf getSparkConfiguration(Properties props) {
    SparkConf sparkConf = new SparkConf();

    if (props.getProperty("spark.dynamicAllocation.enabled", "false").equals("true")) {
      sparkConf.set("spark.dynamicAllocation.enabled", "true");
    }
    else if (props.containsKey("application.executors")) {
      sparkConf.set("spark.executor.instances", props.getProperty("application.executors"));
    }

    if (props.containsKey("application.executor.cores")) {
      sparkConf.set("spark.executor.cores", props.getProperty("application.executor.cores"));
    }
    if (props.containsKey("application.executor.memory")) {
      sparkConf.set("spark.executor.memory", props.getProperty("application.executor.memory"));
    }

    // Override the Spark SQL shuffle partitions with the number of cores, if known.
    if (props.containsKey("application.executors") && props.containsKey("application.executor.cores")) {
      int executors = Integer.parseInt(props.getProperty("application.executors"));
      int executorCores = Integer.parseInt(props.getProperty("application.executor.cores"));
      Integer shufflePartitions = executors * executorCores;

      sparkConf.set("spark.sql.shuffle.partitions", shufflePartitions.toString());
    }

    // Allow the user to provide any Spark configuration and we will just pass it on. These can
    // also override any of the configurations above.
    for (String propertyName : props.stringPropertyNames()) {
      if (propertyName.startsWith("application.spark.conf.")) {
        String sparkConfigName = propertyName.substring("application.spark.conf.".length());
        String sparkConfigValue = props.getProperty(propertyName);

        sparkConf.set(sparkConfigName, sparkConfigValue);
      }
    }

    return sparkConf;
  }

  private static boolean doesRepartition(Properties props) {
    return Boolean.parseBoolean(props.getProperty("source.repartition", "false"));
  }

  private static <T> JavaRDD<T> repartition(JavaRDD<T> batch, Properties props) {
    int numPartitions = Integer.parseInt(props.getProperty("source.repartition.partitions"));
    return batch.repartition(numPartitions);
  }

  // The Spark SQL table name for the stream
  private static String getTableName(Properties props) {
    return DEFAULT_TABLE_NAME;
  }

  private static boolean hasAtLeastOneDeriver(Set<Flow> flows) {
    for (Flow flow : flows) {
      if (flow.hasDeriver()) {
        return true;
      }
    }
    return false;
  }
}
