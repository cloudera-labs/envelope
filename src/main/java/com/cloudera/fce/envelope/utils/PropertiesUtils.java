package com.cloudera.fce.envelope.utils;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Convenience utility methods for working with application properties.
 */
public class PropertiesUtils {
    
    /**
     * Load the properties for the application.
     * @param configurationPath The local path of the properties file.
     * @return The properties object.
     */
    public static Properties loadProperties(String configurationPath) throws Exception {
        Properties props = new Properties();
        
        try (FileInputStream inputStream = new FileInputStream(configurationPath)) {
            props.load(inputStream);
        }
        
        return props;
    }
    
    /**
     * Extract only the properties that match a prefix, and with the prefix removed.
     * @param props All of the properties to extract from.
     * @param prefix The prefix to filter on.
     * @return The prefix properties.
     */
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
    
    /**
     * Convert a comma-separated-values property to a list.
     * @param props The properties that the property is contained in. 
     * @param propertyName The property name that maps to the comma-separated-value.
     * @return The list of values from the property.
     */
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
