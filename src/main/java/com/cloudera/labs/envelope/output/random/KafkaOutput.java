package com.cloudera.labs.envelope.output.random;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

public class KafkaOutput extends RandomWriteOutput {
    
    public static final String BROKERS_CONFIG_NAME = "brokers";
    public static final String TOPIC_CONFIG_NAME = "topic";
    public static final String FIELD_DELIMITER_CONFIG_NAME = "field.delimiter";
    
    public KafkaOutput(Config config) {
        super(config);
    }
    
    @Override
    public void applyMutations(List<PlannedRow> planned) {
        String brokers = config.getString(BROKERS_CONFIG_NAME);

        Properties producerProps = new Properties();
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("bootstrap.servers", brokers);
        
        Serializer<String> serializer = new StringSerializer();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps, serializer, serializer);
        
        String topic = config.getString(TOPIC_CONFIG_NAME);
        String delimiter = getDelimiter();
        Joiner joiner = Joiner.on(delimiter);
        
        List<Object> values = Lists.newArrayList();
        
        for (PlannedRow plan : planned) {
            if (plan.getMutationType().equals(MutationType.INSERT)) {
                values.clear();
                for (int fieldIndex = 0; fieldIndex < plan.getRow().size(); fieldIndex++) {
                    values.add(plan.getRow().get(fieldIndex));
                }
                
                String message = joiner.join(values);
                
                producer.send(new ProducerRecord<String, String>(topic, message));
            }
        }
        
        producer.close();
    }
    
    @Override
    public Set<MutationType> getSupportedMutationTypes() {
        return Sets.newHashSet(MutationType.INSERT);
    }
    
    private String getDelimiter() {
        if (!config.hasPath(FIELD_DELIMITER_CONFIG_NAME)) return ",";
        
        String delimiter = config.getString(FIELD_DELIMITER_CONFIG_NAME);
        
        if (delimiter.startsWith("chars:")) {
            String[] codePoints = delimiter.substring("chars:".length()).split(",");
            
            StringBuilder delimiterBuilder = new StringBuilder();
            for (String codePoint : codePoints) {
                delimiterBuilder.append(Character.toChars(Integer.parseInt(codePoint)));
            }
            
            return delimiterBuilder.toString();
        }
        else {
            return delimiter;
        }
    }
    
}
