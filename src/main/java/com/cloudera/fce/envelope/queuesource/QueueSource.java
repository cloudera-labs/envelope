package com.cloudera.fce.envelope.queuesource;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

@SuppressWarnings("serial")
public abstract class QueueSource implements Serializable {
    
    protected Properties props;
    
    public QueueSource(Properties props) {
        this.props = props;
    }
    
    public abstract JavaDStream<GenericRecord> dStreamFor(JavaStreamingContext jssc, Properties props) throws Exception;
    
    public abstract void enqueueMessage(String queue, String key, String message);
    public void enqueueStringMessage(String queue, String message) {
        enqueueMessage(queue, null, message);
    }
    
    public abstract void enqueueMessage(String queue, byte[] key, byte[] message);
    public void enqueueStringMessage(String queue, byte[] message) {
        enqueueMessage(queue, null, message);
    }
    
    public abstract Schema getSchema() throws Exception;
    
    public static QueueSource queueSourceFor(Properties props) throws Exception {
        QueueSource qs = null;
        
        String queueSourceName = props.getProperty("source");
        
        if (queueSourceName.equals("kafka")) {
            qs = new KafkaQueueSource(props);
        }
        else {
            Class<?> clazz = Class.forName(queueSourceName);
            Constructor<?> constructor = clazz.getConstructor();
            qs = (QueueSource)constructor.newInstance(props);
        }
        
        return qs;
    }
    
}
