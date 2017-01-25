package com.cloudera.labs.envelope.input;

import org.apache.spark.sql.DataFrame;

import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;

public class HiveInput implements BatchInput {
    
    public static final String TABLE_CONFIG_NAME = "table";

    private Config config;
    
    @Override
    public void configure(Config config) {
        this.config = config;
        
        if (!config.hasPath(TABLE_CONFIG_NAME)) {
            throw new RuntimeException("Hive input requires '" + TABLE_CONFIG_NAME + "' property");
        }
    }

    @Override
    public DataFrame read() throws Exception {
        String tableName = config.getString(TABLE_CONFIG_NAME);
        
        return Contexts.getHiveContext().read().table(tableName);
    }

}
