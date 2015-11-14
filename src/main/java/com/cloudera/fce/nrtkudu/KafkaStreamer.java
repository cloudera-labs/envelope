package com.cloudera.fce.nrtkudu;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import kafka.serializer.StringDecoder;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class KafkaStreamer
{
    @SuppressWarnings("serial")
    public static void main(String[] args)
    {
        // The Kudu masters.
        final String storageConnection = args[0];
        // The Kafka brokers.
        final String brokers = args[1];
        // The Kafka topics to receive messages from.
        final String topics = args[2];
        // Message type in the topic. This will correlate to which decoder and encoders are used.
        final String messageType = args[3];
        // The number of seconds per micro-batch.
        final String batchSeconds = args[4];
        
        SparkConf sparkConf = new SparkConf().setAppName("NRT Kudu -- " + messageType);
        
        JavaStreamingContext jssc = new JavaStreamingContext(
            sparkConf, Durations.seconds(Integer.parseInt(batchSeconds))
        );
        
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        
        HashSet<String> topicsSet = new HashSet<>();
        String[] topicArray = topics.split(",");
        for (String topic : topicArray) {
            topicsSet.add(topic);
        }
        
        // Assume that the Kafka topic provides messages encoded as strings.
        // TODO: allow binary messages
        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(
            jssc, String.class, String.class, StringDecoder.class, StringDecoder.class,
            kafkaParams, topicsSet
        );
        
        // This is what we want to do each micro-batch.
        stream.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, String> batch) throws Exception {
                // Group the messages by natural key so that we ensure natural keys cannot span RDD
                // partitions, which would lead to race conditions in updates. This also allows us
                // to control parallelism across any number of executors rather than just by the
                // parallelism of the upstream Kafka topic partitions.
                // TODO: do we need to specify the number of partitions?
                JavaPairRDD<Object, Iterable<Tuple2<String, String>>> messagesByKey =
                        batch.groupBy(new Function<Tuple2<String, String>, Object>()
                {
                    private Decoder decoder = decoderFor(messageType);
                    
                    @Override
                    public Object call(Tuple2<String, String> keyedMessage) throws Exception {
                        return decoder.extractGroupByKey(keyedMessage._1, keyedMessage._2);
                    }
                });
                
                // Process the messages for each micro-batch partition as a single unit so that we
                // can 'bulk' update Kudu, rather than committing Kudu operations every key.
                messagesByKey.foreachPartition(new VoidFunction<Iterator<Tuple2<Object, Iterable<Tuple2<String, String>>>>>() {
                    @Override
                    public void call(Iterator<Tuple2<Object, Iterable<Tuple2<String, String>>>> keyIterator) throws Exception {
                      long start = System.currentTimeMillis();
                      
                      // Decode the Kafka messages into Avro records key by key.
                      Decoder decoder = decoderFor(messageType);
                      List<GenericRecord> decodedInput = Lists.newArrayList();
                      while (keyIterator.hasNext()) {
                          decodedInput.addAll(decoder.decode(keyIterator.next()._2));
                      }
                      
                      // Encode the Avro records into the storage layer.
                      // There is an encoder per storage table.
                      List<Encoder> encoders = encodersFor(messageType, storageConnection);
                      for (Encoder encoder : encoders) {
                          encoder.encode(decodedInput);
                      }
                      
                      System.out.println("Total batch time: " + (System.currentTimeMillis() - start));
                      System.out.println("-----------------------------------------------------");
                    }
                });
                
                // We won't need this when SPARK-4557 is resolved.
                return null;
            }
        });
        
        // Do the thing.
        jssc.start();
        jssc.awaitTermination();
    }
    
    private static Decoder decoderFor(String messageType) throws Exception {
        Decoder decoder = null;
        
        // This is done with reflection to show how it might work with pluggable decoders.
        if (messageType.equals("FIX")) {
            Class<?> clazz = Class.forName("com.cloudera.fce.nrtkudu.fix.FIXDecoder");
            Constructor<?> constructor = clazz.getConstructor();
            decoder = (Decoder)constructor.newInstance();
        }
        
        return decoder;
    }
    
    private static List<Encoder> encodersFor(String messageType, String connection) throws Exception {
        String fixEncoderClassNames = "com.cloudera.fce.nrtkudu.fix.OrdersKuduEncoder,com.cloudera.fce.nrtkudu.fix.RawFIXKuduEncoder";
        
        List<Encoder> encoders = Lists.newArrayList();
        
        if (messageType.equals("FIX")) {
            for (String encoderClassName : fixEncoderClassNames.split(",")) {
                Class<?> clazz = Class.forName(encoderClassName);
                Constructor<?> constructor = clazz.getConstructor();
                Encoder encoder = (Encoder)constructor.newInstance();
                encoder.setConnection(connection);
                encoders.add(encoder);
            }
        }
        
        return encoders;
    }
}