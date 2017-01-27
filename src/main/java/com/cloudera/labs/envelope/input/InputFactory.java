package com.cloudera.labs.envelope.input;

import java.lang.reflect.Constructor;

import com.typesafe.config.Config;

public class InputFactory {
    
    public static final String TYPE_CONFIG_NAME = "type";

    public static Input create(Config config) throws Exception {
        if (!config.hasPath(TYPE_CONFIG_NAME)) {
            throw new RuntimeException("Input type not specified");
        }
        
        String inputType = config.getString(TYPE_CONFIG_NAME);
        
        Input input;
        
        switch (inputType) {
            case "kafka":
                input = new KafkaInput();
                break;
            case "kudu":
                input = new KuduInput();
                break;
            case "filesystem":
                input = new FileSystemInput();
                break;
            case "hive":
                input = new HiveInput();
                break;
            case "jdbc":
                input = new JdbcInput();
                break;
            default:
                Class<?> clazz = Class.forName(inputType);
                Constructor<?> constructor = clazz.getConstructor();
                input = (Input)constructor.newInstance();
        }
        
        input.configure(config);
        
        return input;
    }
    
}
