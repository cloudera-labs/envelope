package com.cloudera.labs.envelope.source;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.cloudera.labs.envelope.translate.Translator;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * A queue source implementation for Apache Kafka. Uses the built-in Kafka direct connector
 * in Apache Spark Streaming.
 */
@SuppressWarnings("serial")
public class KafkaQueueSource extends QueueSource {
    
    private KafkaProducer<String, String> stringProducer;
    private KafkaProducer<byte[], byte[]> byteArrayProducer;
    
    public KafkaQueueSource(Properties props) {
        super(props);
    }
    
    @Override
    public JavaDStream<GenericRecord> dStreamFor(JavaStreamingContext jssc, final Properties props) throws Exception {
        Map<String, String> kafkaParams = Maps.newHashMap();
        
        final String brokers = props.getProperty("source.kafka.brokers");
        kafkaParams.put("metadata.broker.list", brokers);

        final String offset = props.getProperty("source.kafka.offset.reset");
        if(offset!=null) {
            kafkaParams.put("auto.offset.reset", offset);
        }

        final String topics = props.getProperty("source.kafka.topics");
        Set<String> topicsSet = Sets.newHashSet(topics.split(","));
        
        String encoding = props.getProperty("source.kafka.encoding");
        JavaDStream<GenericRecord> dStream = null;
        
        if (encoding.equals("string")) {
            JavaPairDStream<String, String> stringDStream = KafkaUtils.createDirectStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
            
            dStream = stringDStream.map(new Function<Tuple2<String, String>, GenericRecord>() {
                Translator translator;
                
                @Override
                public GenericRecord call(Tuple2<String, String> kafkaKeyAndMessage) throws Exception {
                    String kafkaKey = kafkaKeyAndMessage._1;
                    String kafkaMessage = kafkaKeyAndMessage._2;
                    
                    if (translator == null) {
                        translator = Translator.translatorFor(props);
                    }
                    
                    return translator.translate(kafkaKey, kafkaMessage);
                }
            });
        }
        else if (encoding.equals("bytearray")) {
            JavaPairDStream<byte[], byte[]> byteArrayDStream = KafkaUtils.createDirectStream(
                    jssc, byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class, kafkaParams, topicsSet);
            
            dStream = byteArrayDStream.map(new Function<Tuple2<byte[], byte[]>, GenericRecord>() {
                Translator translator;
                
                @Override
                public GenericRecord call(Tuple2<byte[], byte[]> kafkaKeyAndMessage) throws Exception {
                    byte[] kafkaKey = kafkaKeyAndMessage._1;
                    byte[] kafkaMessage = kafkaKeyAndMessage._2;
                    
                    if (translator == null) {
                        translator = Translator.translatorFor(props);
                    }
                    
                    return translator.translate(kafkaKey, kafkaMessage);
                }
            });
        }
        
        return dStream;
    }
    
    public void enqueueMessage(String queue, String key, String message) {
        if (stringProducer == null) {
            initializeProducer();
        }
        
        stringProducer.send(new ProducerRecord<String, String>(queue, key, message));
    }
    
    @Override
    public void enqueueMessage(String queue, byte[] key, byte[] message) {
        if (byteArrayProducer == null) {
            initializeProducer();
        }
        
        byteArrayProducer.send(new ProducerRecord<byte[], byte[]>(queue, key, message));
    }
    
    @Override
    public Schema getSchema() throws Exception {
        return Translator.translatorFor(props).getSchema();
    }
    
    private void initializeProducer() {
        String encoding = props.getProperty("source.kafka.encoding");
        final String brokers = props.getProperty("source.kafka.brokers");
        
        final Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        
        if (encoding.equals("string")) {
            Serializer<String> serializer = new StringSerializer();
            stringProducer = new KafkaProducer<String, String>(props, serializer, serializer);
        }
        else if (encoding.equals("bytearray")) {
            Serializer<byte[]> serializer = new ByteArraySerializer();
            byteArrayProducer = new KafkaProducer<byte[], byte[]>(props, serializer, serializer);
        }
    }

}
