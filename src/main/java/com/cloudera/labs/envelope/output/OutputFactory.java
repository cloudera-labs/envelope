package com.cloudera.labs.envelope.output;

import java.lang.reflect.Constructor;

import com.typesafe.config.Config;

public class OutputFactory {
    
    public static final String TYPE_CONFIG_NAME = "type";
        
    public static Output create(Config config) throws Exception {
        if (!config.hasPath(TYPE_CONFIG_NAME)) {
            throw new RuntimeException("Output type not specified");
        }
        
        String outputType = config.getString(TYPE_CONFIG_NAME);
        
        Output output;
        
        switch (outputType) {
            case "kudu":
                output = new KuduOutput();
                break;
            case "kafka":
                output = new KafkaOutput();
                break;
            case "log":
                output = new LogOutput();
                break;
            case "hive":
                output = new HiveOutput();
                break;
            case "filesystem":
                output = new FileSystemOutput();
                break;
            default:
                Class<?> clazz = Class.forName(outputType);
                Constructor<?> constructor = clazz.getConstructor();
                output = (Output)constructor.newInstance();
        }
        
        output.configure(config);
        
        return output;
    }
    
}
