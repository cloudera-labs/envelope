package com.cloudera.labs.envelope.derive;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.spark.sql.DataFrame;

import com.typesafe.config.Config;

public abstract class Deriver {
    
    public static final String TYPE_CONFIG_NAME = "type";

    protected Config config;
    
    public Deriver(Config config) {
        this.config = config;
    }
    
    public abstract DataFrame derive(Map<String, DataFrame> dependencies) throws Exception;
    
    public static Deriver deriverFor(Config config) throws Exception {
        if (!config.hasPath(TYPE_CONFIG_NAME)) {
            throw new RuntimeException("Deriver type not specified");
        }
        
        String deriverType = config.getString(TYPE_CONFIG_NAME);
        
        Deriver deriver;
        
        switch (deriverType) {
            case "sql":
                deriver = new SQLDeriver(config);
                break;
            case "passthrough":
                deriver = new PassthroughDeriver(config);
                break;
            case "nest":
                deriver = new NestDeriver(config);
                break;
            default:
                Class<?> clazz = Class.forName(deriverType);
                Constructor<?> constructor = clazz.getConstructor(Config.class);
                deriver = (Deriver)constructor.newInstance(config);
        }
        
        return deriver;
    }
    
}
