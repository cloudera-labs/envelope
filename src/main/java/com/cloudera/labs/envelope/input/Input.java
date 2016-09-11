package com.cloudera.labs.envelope.input;

import java.lang.reflect.Constructor;

import com.cloudera.labs.envelope.input.batch.FileSystemInput;
import com.cloudera.labs.envelope.input.batch.HiveInput;
import com.cloudera.labs.envelope.input.batch.KuduInput;
import com.cloudera.labs.envelope.input.stream.KafkaInput;
import com.typesafe.config.Config;

public abstract class Input {
    
    public static final String TYPE_CONFIG_NAME = "type";
    
    protected Config config;
    
    public Input(Config config) {
        this.config = config;
    }
    
    public static Input inputFor(Config config) throws Exception {
        if (!config.hasPath(TYPE_CONFIG_NAME)) {
            throw new RuntimeException("Input type not specified");
        }
        
        String inputType = config.getString(TYPE_CONFIG_NAME);
        
        Input input;
        
        switch (inputType) {
            case "kafka":
                input = new KafkaInput(config);
                break;
            case "kudu":
                input = new KuduInput(config);
                break;
            case "filesystem":
                input = new FileSystemInput(config);
                break;
            case "hive":
                input = new HiveInput(config);
                break;
            default:
                Class<?> clazz = Class.forName(inputType);
                Constructor<?> constructor = clazz.getConstructor(Config.class);
                input = (Input)constructor.newInstance(config);
        }
        
        return input;
    }
    
}
