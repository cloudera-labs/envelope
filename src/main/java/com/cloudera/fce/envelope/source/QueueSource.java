package com.cloudera.fce.envelope.source;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Abstract class for queue sources to extend.
 */
@SuppressWarnings("serial")
public abstract class QueueSource implements Serializable {
    
    /**
     * The properties of the queue source.
     */
    protected Properties props;
    
    public QueueSource(Properties props) {
        this.props = props;
    }
    
    /**
     * The Spark DStream that represents the stream of the records arriving from the queue source.
     * @param jssc The Spark Streaming context to build the DStream from.
     * @param props The properties of the queue source.
     * @return The DStream for the queue source.
     */
    public abstract JavaDStream<GenericRecord> dStreamFor(JavaStreamingContext jssc, Properties props) throws Exception;
    
    /**
     * Enqueue a string message on to a queue of the queue source.
     * @param queue The name of the queue.
     * @param key The string key of the message.
     * @param message The string message.
     */
    public abstract void enqueueMessage(String queue, String key, String message);
    
    /**
     * Enqueue a string message, with a null key, on to a queue of the queue source.
     * @param queue The name of the queue.
     * @param message The string message.
     */
    public void enqueueStringMessage(String queue, String message) {
        enqueueMessage(queue, null, message);
    }
    
    /**
     * Enqueue a binary message on to a queue of the queue source.
     * @param queue The name of the queue.
     * @param key The binary key of the message.
     * @param message The binary message.
     */
    public abstract void enqueueMessage(String queue, byte[] key, byte[] message);
    
    /**
     * Enqueue a binary message, with a null key, on to a queue of the queue source.
     * @param queue The name of the queue.
     * @param message The binary message.
     */
    public void enqueueStringMessage(String queue, byte[] message) {
        enqueueMessage(queue, null, message);
    }
    
    /**
     * The schema of the messages of the queue source.
     * @return The Avro schema equivalent of the schema of the queue source.
     */
    public abstract Schema getSchema() throws Exception;
    
    /**
     * The queue source for the application.
     * @param props The properties of the queue source.
     * @return The queue source.
     */
    public static QueueSource queueSourceFor(Properties props) throws Exception {
        QueueSource qs = null;
        
        String queueSourceName = props.getProperty("source");
        
        if (queueSourceName.equals("kafka")) {
            qs = new KafkaQueueSource(props);
        }
        else {
            Class<?> clazz = Class.forName(queueSourceName);
            Constructor<?> constructor = clazz.getConstructor(Properties.class);
            qs = (QueueSource)constructor.newInstance(props);
        }
        
        return qs;
    }
    
}
