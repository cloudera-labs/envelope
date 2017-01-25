package com.cloudera.labs.envelope.derive;

import java.lang.reflect.Constructor;

import com.typesafe.config.Config;

public class DeriverFactory {
    
    public static final String TYPE_CONFIG_NAME = "type";
    
    public static Deriver create(Config config) throws Exception {
        if (!config.hasPath(TYPE_CONFIG_NAME)) {
            throw new RuntimeException("Deriver type not specified");
        }
        
        String deriverType = config.getString(TYPE_CONFIG_NAME);
        
        Deriver deriver;
        
        switch (deriverType) {
            case "sql":
                deriver = new SQLDeriver();
                break;
            case "passthrough":
                deriver = new PassthroughDeriver();
                break;
            case "nest":
                deriver = new NestDeriver();
                break;
            default:
                Class<?> clazz = Class.forName(deriverType);
                Constructor<?> constructor = clazz.getConstructor();
                deriver = (Deriver)constructor.newInstance();
        }
        
        deriver.configure(config);
        
        return deriver;
    }
    
}
