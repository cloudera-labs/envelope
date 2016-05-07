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

import com.cloudera.fce.envelope.queuesource.QueueSource;
import com.cloudera.fce.envelope.utils.PropertiesUtils;
import com.cloudera.fce.envelope.utils.SparkSQLAvroUtils;
import com.google.common.collect.Maps;

@SuppressWarnings("serial")
public class Streamer
{
    
    public static void main(String[] args) throws Exception
    {
        final Properties props = PropertiesUtils.loadProperties(args[0]);
        
        JavaStreamingContext jssc = getStreamingContext(props);
        
        final QueueSource qs = QueueSource.queueSourceFor(props);
        JavaDStream<GenericRecord> stream = qs.dStreamFor(jssc, props);
        
        if (doesExpandToWindow(props)) {
            stream = expandToWindow(stream, props);
        }
        
        final SQLContext sqlc = new SQLContext(jssc.sparkContext());
        
        // This is what we want to do each micro-batch.
        stream.foreachRDD(new Function<JavaRDD<GenericRecord>, Void>() {
            @Override
            public Void call(JavaRDD<GenericRecord> streamRecords) throws Exception {
                if (doesRepartition(props)) {
                    streamRecords = repartition(streamRecords, props);
                }
                
                DataFrame streamDataFrame = null;
                Map<String, DataFrame> lookupDataFrames = null;
                Set<Flow> flows = Flow.flowsFor(props);
                if (hasAtLeastOneDeriver(flows)) {
                    Schema streamSchema = qs.getSchema();
                    streamDataFrame = SparkSQLAvroUtils.dataFrameForRecords(streamRecords, streamSchema, sqlc);
                    streamDataFrame.registerTempTable(getStreamTableName(props));
                    streamDataFrame.persist(StorageLevel.MEMORY_ONLY());
                    
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
                
                for (Flow flow : flows) {
                    if (flow.hasDeriver()) {
                        flow.runFlow(streamDataFrame, lookupDataFrames);
                    }
                    else {
                        flow.runFlow(streamRecords);
                    }
                }
                
                if (hasAtLeastOneDeriver(flows)) {
                    streamDataFrame.unpersist();
                    for (DataFrame lookupDataFrame : lookupDataFrames.values()) {
                        lookupDataFrame.unpersist();
                    }
                }
                
                // SPARK-4557
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
        
        sparkConf.set("spark.dynamicAllocation.enabled", "false");
        sparkConf.set("spark.streaming.backpressure.enabled", "true");
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5000");
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
        if (props.containsKey("application.executors") && props.containsKey("application.executor.cores")) {
            int executors = Integer.parseInt(props.getProperty("application.executors"));
            int executorCores = Integer.parseInt(props.getProperty("application.executor.cores"));
            Integer shufflePartitions = executors * executorCores;
            
            sparkConf.set("spark.sql.shuffle.partitions", shufflePartitions.toString());
        }
        
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
        return Boolean.parseBoolean(props.getProperty("application.window.enable", "false"));
    }
    
    private static <T> JavaDStream<T> expandToWindow(JavaDStream<T> stream, Properties props) {        
        int windowDuration = Integer.parseInt(props.getProperty("application.window.milliseconds"));
        
        return stream.window(new Duration(windowDuration));
    }
    
    private static boolean doesRepartition(Properties props) {
        return Boolean.parseBoolean(props.getProperty("source.repartition", "true"));
    }
    
    private static <T> JavaRDD<T> repartition(JavaRDD<T> records, Properties props) {
        int numPartitions = Integer.parseInt(props.getProperty("source.repartition.partitions", "2"));
        
        return records.repartition(numPartitions);
    }
    
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
