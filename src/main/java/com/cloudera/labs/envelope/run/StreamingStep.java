package com.cloudera.labs.envelope.run;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.cloudera.labs.envelope.input.StreamInput;
import com.typesafe.config.Config;

public class StreamingStep extends DataStep {
    
    public static final String REPARTITION_PROPERTY = "input.repartition";
    public static final String REPARTITION_NUM_PARTITIONS_PROPERTY = "input.repartition.partitions";
    public static final String WINDOW_ENABLED_PROPERTY = "window.enabled";
    public static final String WINDOW_MILLISECONDS_PROPERTY = "window.milliseconds";

    public StreamingStep(String name, Config config) throws Exception {
        super(name, config);
    }

    public JavaDStream<Row> getStream() throws Exception {
        JavaDStream<Row> stream = ((StreamInput)input).getDStream();
        
        if (doesExpandToWindow(config)) {
            stream = expandToWindow(stream, config);
        }
        
        if (doesRepartition(config)) {
            stream = repartition(stream, config);
        }
        
        return stream;
    }
    
    public StructType getSchema() throws Exception {
        StructType schema = ((StreamInput)input).getSchema();
        
        return schema;
    }
    
    private static boolean doesRepartition(Config config) {
        if (!config.hasPath(REPARTITION_PROPERTY)) return false;
        
        return config.getBoolean(REPARTITION_PROPERTY);
    }
    
    private static <T> JavaDStream<T> repartition(JavaDStream<T> stream, Config config) {
        int numPartitions = config.getInt(REPARTITION_NUM_PARTITIONS_PROPERTY);
        
        return stream.repartition(numPartitions);
    }
    
    private static boolean doesExpandToWindow(Config config) {
        if (!config.hasPath(WINDOW_ENABLED_PROPERTY)) return false;
        
        return config.getBoolean(WINDOW_ENABLED_PROPERTY);
    }
    
    private static <T> JavaDStream<T> expandToWindow(JavaDStream<T> stream, Config config) {        
        int windowDuration = config.getInt(WINDOW_MILLISECONDS_PROPERTY);
        
        return stream.window(new Duration(windowDuration));
    }
    
}
