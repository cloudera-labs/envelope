package com.cloudera.fce.envelope;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import com.cloudera.fce.envelope.source.QueueSource;
import com.cloudera.fce.envelope.utils.PropertiesUtils;
import com.cloudera.fce.envelope.utils.SparkSQLAvroUtils;
import com.google.common.collect.Maps;

/**
 * The entry point into Envelope. Configures the Spark Streaming job and runs it indefinitely.
 */
@SuppressWarnings("serial")
public class Streamer
{   
    public static void main(String[] args) throws Exception
    {
        // Retrieve the properties for the pipeline being executed
        final Properties props = PropertiesUtils.loadProperties(args[0]);
        
        // Instantiate the Spark Streaming job
        JavaStreamingContext jssc = getStreamingContext(props);
        
        // Initialize the stream
        final QueueSource qs = QueueSource.queueSourceFor(props);
        JavaDStream<GenericRecord> stream = qs.dStreamFor(jssc, props);
        
        // If required, expand the stream window beyond the micro-batch length
        if (doesExpandToWindow(props)) {
            stream = expandToWindow(stream, props);
        }
        
        // If required, repartition the stream across the executors
        if (doesRepartition(props)) {
            stream = repartition(stream, props);
        }
        
        // Initialize Spark SQL
        final SQLContext sqlc = new SQLContext(jssc.sparkContext());
        
        // This is what we want to do each micro-batch
        stream.foreachRDD(new Function<JavaRDD<GenericRecord>, Void>() {
            @Override
            public Void call(JavaRDD<GenericRecord> streamRecords) throws Exception {
                DataFrame streamDataFrame = null;
                Map<String, DataFrame> lookupDataFrames = null;
                
                // Create the flows that we are going to run for the micro-batch
                Set<Flow> flows = Flow.flowsFor(props);
                
                // We only use DataFrames in derivers, so we only need to convert to a DataFrame
                // if there is at least one deriver in the pipeline
                if (hasAtLeastOneDeriver(flows)) {
                    // Convert the stream to a DataFrame
                    Schema streamSchema = qs.getSchema();
                    streamDataFrame = SparkSQLAvroUtils.dataFrameForRecords(streamRecords, streamSchema, sqlc);
                    streamDataFrame.registerTempTable(getStreamTableName(props));
                    streamDataFrame.persist(StorageLevel.MEMORY_ONLY());
                    
                    // Load all the required lookups for the micro-batch
                    lookupDataFrames = Maps.newHashMap();
                    Set<Lookup> lookups = Lookup.lookupsFor(props);
                    for (Lookup lookup : lookups) {
                        JavaRDD<GenericRecord> lookupRecords = lookup.getLookupRecordsFor(streamRecords);
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
                        flow.runFlow(streamDataFrame, lookupDataFrames);
                    }
                    // Flows that don't have derivers do not need to pay the penalty of converting
                    // into and back from DataFrames, so we just skip that part altogether
                    else {
                        flow.runFlow(streamRecords);
                    }
                }
                
                // Remove all the cached DataFrames from memory
                if (hasAtLeastOneDeriver(flows)) {
                    streamDataFrame.unpersist();
                    for (DataFrame lookupDataFrame : lookupDataFrames.values()) {
                        lookupDataFrame.unpersist();
                    }
                }
                
                // Required due to SPARK-4557
                return null;
            }
        });
        
        // Do the thing.
        jssc.start();
        jssc.awaitTermination();
    }
    
    private static JavaStreamingContext getStreamingContext(final Properties props) {
        final SparkConf sparkConf = getSparkConfiguration(props);
        
        String applicationName = props.getProperty("application.name");
        sparkConf.setAppName(applicationName);
        
        int batchMilliseconds = Integer.parseInt(props.getProperty("application.batch.milliseconds"));
        final Duration batchDuration = Durations.milliseconds(batchMilliseconds);
        
        JavaStreamingContext jssc;
        boolean toCheckpoint = Boolean.parseBoolean(props.getProperty("application.checkpoint.enabled", "false"));
        if (toCheckpoint) {
            String checkpointPath = props.getProperty("application.checkpoint.path");
            JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
                @Override
                public JavaStreamingContext create() {
                    return new JavaStreamingContext(sparkConf, batchDuration);
                }
            };
            jssc = JavaStreamingContext.getOrCreate(checkpointPath, factory);
            jssc.checkpoint(checkpointPath);
        }
        else {
            jssc = new JavaStreamingContext(sparkConf, batchDuration);
        }
        
        return jssc;
    }
    
    private static SparkConf getSparkConfiguration(Properties props) {
        SparkConf sparkConf = new SparkConf();
        
        // Dynamic allocation should not be used for Spark Streaming jobs because the latencies
        // of the resource requests are too long.
        sparkConf.set("spark.dynamicAllocation.enabled", "false");
        // Spark Streaming back-pressure helps automatically tune the size of the micro-batches so
        // that they don't breach the micro-batch length.
        sparkConf.set("spark.streaming.backpressure.enabled", "true");
        // Rate limit the micro-batches when using Kafka to 5000 records per Kafka topic partition
        // per second. Without this we could end up with arbitrarily large initial micro-batches
        // for existing topics.
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5000");
        // Override the Spark SQL shuffle partitions with the default number of cores. Otherwise
        // the default is typically 200 partitions, which is very high for micro-batches.
        sparkConf.set("spark.sql.shuffle.partitions", "2");
        
        if (props.containsKey("application.executors")) {
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
    
    private static boolean doesExpandToWindow(Properties props) {
        return Boolean.parseBoolean(props.getProperty("application.window.enabled", "false"));
    }
    
    private static <T> JavaDStream<T> expandToWindow(JavaDStream<T> stream, Properties props) {        
        int windowDuration = Integer.parseInt(props.getProperty("application.window.milliseconds"));
        
        return stream.window(new Duration(windowDuration));
    }
    
    private static boolean doesRepartition(Properties props) {
        return Boolean.parseBoolean(props.getProperty("source.repartition", "false"));
    }
    
    private static <T> JavaDStream<T> repartition(JavaDStream<T> stream, Properties props) {
        int numPartitions = Integer.parseInt(props.getProperty("source.repartition.partitions"));
        
        return stream.repartition(numPartitions);
    }
    
    // The Spark SQL table name for the stream
    private static String getStreamTableName(Properties props) {
        return "stream";
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
