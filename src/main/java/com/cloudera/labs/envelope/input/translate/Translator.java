package com.cloudera.labs.envelope.input.translate;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import com.typesafe.config.Config;

public interface Translator<T> { 
        
    void configure(Config config);
    
    Row translate(T key, T message) throws Exception;
    
    StructType getSchema();
    
}
