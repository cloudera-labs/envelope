package com.cloudera.fce.envelope.queuesource;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.kududb.client.shaded.com.google.common.collect.Sets;

import com.cloudera.fce.envelope.RecordModel;
import com.cloudera.fce.envelope.translator.Translator;
import com.google.common.collect.Maps;

@SuppressWarnings("serial")
public class KafkaQueueSource extends QueueSource {
    
    public KafkaQueueSource(Properties props) {
        super(props);
    }
    
    @Override
    public JavaDStream<GenericRecord> dStreamFor(JavaStreamingContext jssc, final Properties props) throws Exception {
        Map<String, String> kafkaParams = Maps.newHashMap();
        
        final String brokers = props.getProperty("source.kafka.brokers");
        kafkaParams.put("metadata.broker.list", brokers);
        
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
    
    @Override
    public void enqueueStringMessage(String queue, String key, String message) {
        // TODO: do this
    }
    
    @Override
    public Schema getSchema() throws Exception {
        return Translator.translatorFor(props).getSchema();
    }

    @Override
    public RecordModel getRecordModel() throws Exception {
        return Translator.translatorFor(props).getRecordModel();
    }

}
