package com.cloudera.fce.nrtkudu.fix;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class FIXGenerator {
    public static void main(String[] args) throws Exception {        
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("metadata.broker.list", args[0]);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        @SuppressWarnings("resource")
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        
        String key = UUID.randomUUID().toString();
        
        while(true) {
            Random random = new Random(); // random
            int leavesQty = Math.abs(random.nextInt()) % 3000;
            
            while (leavesQty > 0) {
                String tag35 = "D";
                String tag11 = key;
                String tag21 = "2";
                String tag55 = "AAPL";
                String tag54 = "2";
                String tag60 = Long.toString(System.currentTimeMillis());
                String tag38 = Integer.toString(leavesQty);
                String tag40 = "2";
                String tag10 = "0";
                
                String message = String.format(
                        "%d=%s\001%d=%s\001%d=%s\001%d=%s\001%d=%s\001%d=%s\001%d=%s\001%d=%s\001%d=%s",
                        35, tag35, 11, tag11, 21, tag21, 55, tag55, 54, tag54,
                        60, tag60, 38, tag38, 40, tag40, 10, tag10);
                
                producer.send(new ProducerRecord<String, String>(args[1], message));
                
                leavesQty -= Math.abs(random.nextInt()) % 3000;
                
                if (leavesQty <= 0) {
                    Thread.sleep(2);
                    message = String.format(
                            "%d=%s\001%d=%s\001%d=%s\001%d=%s\001%d=%s\001%d=%s\001%d=%s\001%d=%s\001%d=%s",
                            35, tag35, 11, tag11, 21, tag21, 55, tag55, 54, tag54,
                            60, Long.toString(System.currentTimeMillis()), 38, "0", 40, tag40, 10, tag10);
                    producer.send(new ProducerRecord<String, String>(args[1], message));
                }
                
                Thread.sleep(2);
            }
            
            key = UUID.randomUUID().toString();
        }
    }
}
