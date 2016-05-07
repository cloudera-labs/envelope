package com.cloudera.fce.envelope.deriver;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.DataFrame;

import com.cloudera.fce.envelope.utils.PropertiesUtils;

public abstract class Deriver {
    
    protected Properties props;
    
    public Deriver(Properties props) {
        this.props = props;
    }
    
    public abstract DataFrame derive(DataFrame stream, Map<String, DataFrame> lookups) throws Exception;
    
    public static Deriver deriverFor(Properties props) throws Exception {
        String deriverName = props.getProperty("deriver");
        Properties deriverProps = PropertiesUtils.prefixProperties(props, "deriver.");
        
        Deriver deriver = null;
        
        if (deriverName.equals("sql")) {
            deriver = new SQLDeriver(deriverProps);
        }
        else {
            Class<?> clazz = Class.forName(deriverName);
            Constructor<?> constructor = clazz.getConstructor();
            deriver = (Deriver)constructor.newInstance(deriverProps);
        }
        
        return deriver;
    }
    
}
