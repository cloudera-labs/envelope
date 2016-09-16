package com.cloudera.labs.envelope.examples;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Cheap Bluecoat proxy log producer; count number as 'sc-bytes'
 */
public class ProxyGenerator {
    
    public static void main(final String[] args) throws Exception {
        final Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        @SuppressWarnings("resource")
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int count = 1;

        while (true) {
            String logMsg = "2005-05-04 17:16:12 1 45.110.2.82 200 TCP_HIT " + count + " 729 GET http www.example.com " +
                "/wcm/assets/images/imagefileicon.gif - - DIRECT 38.112.92.20 image/gif \"Mozilla/4.0 (compatible; " +
                "MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)\" PROXIED none - 192.16.170.42 SG-HTTP-Service - " +
                "none -";

            producer.send(new ProducerRecord<String, String>(args[1], logMsg));
            count++;

            Thread.sleep(10);
        }
    }
    
}
