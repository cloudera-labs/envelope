package com.cloudera.labs.envelope.input;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.cloudera.labs.envelope.input.translate.TranslateFunction;
import com.cloudera.labs.envelope.input.translate.TranslatorFactory;
import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

public class KafkaInput implements StreamInput {
    
    public static final String BROKERS_CONFIG_NAME = "brokers";
    public static final String TOPICS_CONFIG_NAME = "topics";
    public static final String ENCODING_CONFIG_NAME = "encoding";
    public static final String PARAMETER_CONFIG_PREFIX = "parameter.";

    private Config config;
    
    @Override
    public void configure(Config config) {
        this.config = config;
    }
    
    @Override
    public JavaDStream<Row> getDStream() throws Exception {
        Map<String, String> kafkaParams = Maps.newHashMap();
        
        String brokers = config.getString(BROKERS_CONFIG_NAME);
        kafkaParams.put("metadata.broker.list", brokers);
        
        String topics = config.getString(TOPICS_CONFIG_NAME);
        Set<String> topicsSet = Sets.newHashSet(topics.split(Pattern.quote(",")));
        
        String encoding = config.getString(ENCODING_CONFIG_NAME);
        
        addCustomParams(kafkaParams);
        
        Config translatorConfig = config.getConfig("translator");
        
        JavaStreamingContext jssc = Contexts.getJavaStreamingContext();
        JavaDStream<Row> dStream = null;
        
        if (encoding.equals("string")) {
            JavaPairDStream<String, String> stringDStream = KafkaUtils.createDirectStream(
                    jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
            
            dStream = stringDStream.map(new TranslateFunction<String>(translatorConfig));
        }
        else if (encoding.equals("bytearray")) {
            JavaPairDStream<byte[], byte[]> byteArrayDStream = KafkaUtils.createDirectStream(
                    jssc, byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class, kafkaParams, topicsSet);
            
            dStream = byteArrayDStream.map(new TranslateFunction<byte[]>(translatorConfig));
        }
        else {
            throw new RuntimeException("Invalid Kafka input encoding type. Valid types are 'string' and 'bytearray'.");
        }
        
        return dStream;
    }

    @Override
    public StructType getSchema() throws Exception {
        Config translatorConfig = config.getConfig("translator");
        return TranslatorFactory.create(translatorConfig).getSchema();
    }
    
    private void addCustomParams(Map<String, String> params) {
        for (String propertyName : config.root().keySet()) {
            if (propertyName.startsWith(PARAMETER_CONFIG_PREFIX)) {
                String paramName = propertyName.substring(PARAMETER_CONFIG_PREFIX.length());
                String paramValue = config.getString(propertyName);
                
                params.put(paramName, paramValue);
            }
        }
    }

}
