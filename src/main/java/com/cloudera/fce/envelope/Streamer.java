package com.cloudera.fce.envelope;

import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import com.cloudera.fce.envelope.queuesource.QueueSource;
import com.cloudera.fce.envelope.utils.PropertiesUtils;
import com.cloudera.fce.envelope.utils.RecordUtils;
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
        
        // This is what we want to do each micro-batch.
        stream.foreachRDD(new Function<JavaRDD<GenericRecord>, Void>() {
            @Override
            public Void call(JavaRDD<GenericRecord> batchRecords) throws Exception {
                boolean prePartitioned = Boolean.parseBoolean(props.getProperty("source.partition", "false"));
                if (!prePartitioned) {
                    int numPartitions = Integer.parseInt(props.getProperty("source.partition.number", "1"));
                    batchRecords = partitionByKey(batchRecords, numPartitions, qs.getRecordModel());
                }
                
                JavaRDD<Row> batchRows = SparkSQLAvroUtils.rowsForRecords(batchRecords);
                StructType batchStructType = SparkSQLAvroUtils.structTypeForSchema(qs.getSchema());
                SQLContext sqlc = new SQLContext(jssc.sparkContext()); 
                DataFrame batchDataFrame = sqlc.createDataFrame(batchRows, batchStructType);
                batchDataFrame.registerTempTable("stream");
                batchDataFrame.persist(StorageLevel.MEMORY_ONLY());
                
                for (Flow flow : Flow.flowsFor(props)) {
                    flow.runFlow(batchDataFrame);
                }
                
                batchDataFrame.unpersist();
                
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
        boolean toCheckpoint = Boolean.parseBoolean(props.getProperty("application.checkpoint.enabled", "true"));
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
    
    private static JavaRDD<GenericRecord> partitionByKey(JavaRDD<GenericRecord> unpartitioned,
            int numPartitions, final RecordModel recordModel)
    {
        return unpartitioned
            .groupBy(new Function<GenericRecord, GenericRecord>() {
                @Override
                public GenericRecord call(GenericRecord record) throws Exception {
                    return RecordUtils.subsetRecord(record, recordModel.getKeyFieldNames());
                }
            }, numPartitions)
            .values()
            .flatMap(new FlatMapFunction<Iterable<GenericRecord>, GenericRecord>() {
                @Override
                public Iterable<GenericRecord> call(Iterable<GenericRecord> keyedMessages) {
                    return keyedMessages;
                }
            });
    }

}
