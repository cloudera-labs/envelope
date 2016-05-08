package com.cloudera.fce.envelope.examples;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TrafficGenerator {

    public static void main(final String[] args) throws Exception {
        
        final Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        @SuppressWarnings("resource")
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        Random random = new Random();
        
        while (true) {
            Long timestamp = System.currentTimeMillis();
            Integer numVehicles = random.nextInt(100);
            
            String message = timestamp + "," + numVehicles;
            
            producer.send(new ProducerRecord<String, String>(args[1], message));
            
            Thread.sleep(1000);
        }
        
    }
    
}
