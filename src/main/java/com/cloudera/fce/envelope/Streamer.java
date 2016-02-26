package com.cloudera.fce.envelope;

import java.util.List;
import java.util.Properties;

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
import org.kududb.client.shaded.com.google.common.collect.Lists;

import com.cloudera.fce.envelope.queuesource.QueueSource;
import com.cloudera.fce.envelope.utils.PropertiesUtils;
import com.cloudera.fce.envelope.utils.SparkSQLAvroUtils;

@SuppressWarnings("serial")
public class Streamer
{
    public static void main(String[] args) throws Exception
    {
        final Properties props = PropertiesUtils.loadProperties(args[0]);
        
        final JavaStreamingContext jssc = getStreamingContext(props);
        
        final QueueSource qs = QueueSource.queueSourceFor(props);
        JavaDStream<GenericRecord> stream = qs.dStreamFor(jssc, props);
        
        if (doesExpandToWindow(props)) {
            stream = expandToWindow(stream, props);
        }
        
        final SQLContext sqlc = new SQLContext(jssc.sparkContext());
        
        // This is what we want to do each micro-batch.
        stream.foreachRDD(new Function<JavaRDD<GenericRecord>, Void>() {
            @Override
            public Void call(JavaRDD<GenericRecord> arrivingRecords) throws Exception {
                if (doesRepartition(props)) {
                    arrivingRecords = repartition(arrivingRecords, props);
                }
                
                Schema arrivingSchema = qs.getSchema();
                DataFrame arrivingDataFrame = SparkSQLAvroUtils.dataFrameForRecords(arrivingRecords, arrivingSchema, sqlc);
                arrivingDataFrame.registerTempTable(getStreamTableName(props));
                arrivingDataFrame.persist(StorageLevel.MEMORY_ONLY());
                
                List<DataFrame> lookupDataFrames = Lists.newArrayList();
                for (Lookup lookup : Lookup.lookupsFor(props)) {
                    JavaRDD<GenericRecord> lookupRecords = lookup.getLookupRecordsFor(arrivingRecords);
                    Schema lookupSchema = lookup.getLookupTableSchema();
                    DataFrame lookupDataFrame = SparkSQLAvroUtils.dataFrameForRecords(lookupRecords, lookupSchema, sqlc);
                    lookupDataFrame.registerTempTable(lookup.getLookupTableName());
                    lookupDataFrame.persist(StorageLevel.MEMORY_ONLY());
                    lookupDataFrames.add(lookupDataFrame);
                }
                
                for (Flow flow : Flow.flowsFor(props)) {
                    flow.runFlow(arrivingDataFrame);
                }
                
                arrivingDataFrame.unpersist();
                for (DataFrame lookupDataFrame : lookupDataFrames) {
                    lookupDataFrame.unpersist();
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
        final SparkConf sparkConf = new SparkConf();
        
        String applicationName = props.getProperty("application.name");
        sparkConf.setAppName(applicationName);
        
        int batchMilliseconds = Integer.parseInt(props.getProperty("application.batch.milliseconds"));
        final Duration batchDuration = Durations.milliseconds(batchMilliseconds);
        
        sparkConf.set("spark.dynamicAllocation.enabled", "false");
        sparkConf.set("spark.streaming.backpressure.enabled", "true");
        
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
    
    private static boolean doesExpandToWindow(Properties props) {
        return Boolean.parseBoolean(props.getProperty("application.window.enable", "false"));
    }
    
    private static <T> JavaDStream<T> expandToWindow(JavaDStream<T> stream, Properties props) {        
        int windowDuration = Integer.parseInt(props.getProperty("application.window.milliseconds"));
        
        return stream.window(new Duration(windowDuration));
    }
    
    private static boolean doesRepartition(Properties props) {
        return Boolean.parseBoolean(props.getProperty("source.repartition", "false"));
    }
    
    private static <T> JavaRDD<T> repartition(JavaRDD<T> records, Properties props) {
        int numPartitions = Integer.parseInt(props.getProperty("source.repartition.partitions"));
        
        return records.repartition(numPartitions);
    }
    
    private static String getStreamTableName(Properties props) {
        return "stream";
    }

}
