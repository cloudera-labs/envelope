package com.cloudera.fce.envelope.utils;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class PropertiesUtils {
    
    public static Properties loadProperties(String configurationPath) throws Exception {
        Properties props = new Properties();
        
        try (FileInputStream inputStream = new FileInputStream(configurationPath)) {
            props.load(inputStream);
        }
        
        return props;
    }
    
    public static Properties prefixProperties(Properties props, String prefix) {
        Properties prefixProps = new Properties();
        
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                String keyWithoutPrefix = key.substring(prefix.length());
                prefixProps.setProperty(keyWithoutPrefix, props.getProperty(key));
            }
        }
        
        return prefixProps;
    }
    
    public static List<String> propertyAsList(Properties props, String propertyName) {
        String prop = props.getProperty(propertyName);
        
        if (prop != null) {
            return Arrays.asList(prop.split(Pattern.quote(",")));
        }
        else {
            return null;
        }
    }
    
}
