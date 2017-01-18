package com.cloudera.labs.envelope.output;

import java.lang.reflect.Constructor;
import java.util.Set;

import com.cloudera.labs.envelope.output.bulk.FileSystemOutput;
import com.cloudera.labs.envelope.output.bulk.HiveOutput;
import com.cloudera.labs.envelope.output.bulk.KafkaOutput;
import com.cloudera.labs.envelope.output.bulk.LogOutput;
import com.cloudera.labs.envelope.output.random.KuduOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.typesafe.config.Config;

public abstract class Output {
    
    public static final String TYPE_CONFIG_NAME = "type";

    protected Config config;
    
    public Output(Config config) {
        this.config = config;
    }
    
    public abstract Set<MutationType> getSupportedMutationTypes();
    
    public static Output outputFor(Config config) throws Exception {
        if (!config.hasPath(TYPE_CONFIG_NAME)) {
            throw new RuntimeException("Output type not specified");
        }
        
        String outputType = config.getString(TYPE_CONFIG_NAME);
        
        Output output;
        
        switch (outputType) {
            case "kudu":
                output = new KuduOutput(config);
                break;
            case "kafka":
                output = new KafkaOutput(config);
                break;
            case "log":
                output = new LogOutput(config);
                break;
            case "hive":
                output = new HiveOutput(config);
                break;
            case "filesystem":
                output = new FileSystemOutput(config);
                break;
            default:
                Class<?> clazz = Class.forName(outputType);
                Constructor<?> constructor = clazz.getConstructor(Config.class);
                output = (Output)constructor.newInstance(config);
        }
        
        return output;
    }
    
}
