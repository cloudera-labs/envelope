package com.cloudera.fce.nrtkudu;

import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import kafka.serializer.StringDecoder;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class KafkaStreamer
{
    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception
    {        
        // The properties of the job.
        final Properties props = loadProperties(args[0]);
        
        // The Kafka brokers.
        final String brokers = props.getProperty("kafka.brokers");
        // The Kafka topics to receive messages from.
        final String topics = props.getProperty("kafka.topics");
        // Message type in the topic. This will correlate to which decoder and encoders are used.
        final String applicationName = props.getProperty("application.name");
        // The number of seconds per micro-batch.
        final int batchSeconds = Integer.parseInt(props.getProperty("application.batch.seconds"));
        
        final SparkConf sparkConf = new SparkConf().setAppName("NRT Kudu -- " + applicationName);
        
        String checkpointPath = props.getProperty("application.checkpoint.path");
        JavaStreamingContextFactory factory = new JavaStreamingContextFactory() { 
            @Override
            public JavaStreamingContext create() {
                return new JavaStreamingContext(sparkConf, Durations.seconds(batchSeconds));
            }
        };
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(checkpointPath, factory);
        jssc.checkpoint(checkpointPath);
        
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
                Boolean prePartitioned = Boolean.parseBoolean(props.getProperty("kafka.prepartitioned"));
                
                if (!prePartitioned) {
                    // Partition the messages by natural key so that we ensure natural keys cannot span
                    // RDD partitions, which would lead to race conditions in updates. This also allows
                    // us to control parallelism across any number of executors rather than just by the
                    // parallelism of the upstream Kafka topic partitions.
                    batch = batch
                        // TODO: do we need to specify the number of partitions?
                        .groupBy(new Function<Tuple2<String, String>, Object>() {
                            private Decoder decoder = decoderFor(props);
                            
                            @Override
                            public Object call(Tuple2<String, String> keyedMessage) throws Exception {
                                return decoder.extractGroupByKey(keyedMessage._1, keyedMessage._2);
                            }
                        })
                        .values()
                        .flatMapToPair(new PairFlatMapFunction<Iterable<Tuple2<String, String>>, String, String>() {
                            @Override
                            public Iterable<Tuple2<String, String>> call(Iterable<Tuple2<String, String>> keyedMessages) {
                                return keyedMessages;
                            }
                        });
                }
                
                // Process the messages for each micro-batch partition as a single unit so that we
                // can 'bulk' update Kudu, rather than committing Kudu operations every key.
                batch.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, String>> keyIterator) throws Exception {
                      long start = System.currentTimeMillis();
                      
                      // Decode into Avro records the Kafka messages for each natural key.
                      Decoder decoder = decoderFor(props);
                      List<GenericRecord> decodedInput = Lists.newArrayList();
                      while (keyIterator.hasNext()) {
                          decodedInput.addAll(decoder.decode(keyIterator));
                      }
                      
                      // Encode the Avro records into the storage layer.
                      // There is an encoder per storage table.
                      List<Encoder> encoders = encodersFor(props);
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
    
    private static Decoder decoderFor(Properties props) throws Exception {
        Decoder decoder = null;
        
        Class<?> clazz = Class.forName(props.getProperty("decoder.class"));
        Constructor<?> constructor = clazz.getConstructor();
        decoder = (Decoder)constructor.newInstance();
        
        return decoder;
    }
    
    private static List<Encoder> encodersFor(Properties props) throws Exception {
        String fixEncoderClassNames = props.getProperty("encoder.classes");
        
        List<Encoder> encoders = Lists.newArrayList();
        
        for (String encoderClassName : fixEncoderClassNames.split(",")) {
            Class<?> clazz = Class.forName(encoderClassName);
            Constructor<?> constructor = clazz.getConstructor();
            Encoder encoder = (Encoder)constructor.newInstance();
            encoder.setProperties(props);
            encoders.add(encoder);
        }
        
        return encoders;
    }
    
    private static Properties loadProperties(String configurationPath) throws Exception {
        Properties props = new Properties();
        
        try(FileInputStream inputStream = new FileInputStream(configurationPath)) {
            props.load(inputStream);
        }
        
        return props;
    }
}
