package com.cloudera.labs.envelope.input.batch;

import org.apache.spark.sql.DataFrame;

import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;

public class HiveInput extends BatchInput {
    
    public static final String TABLE_CONFIG_NAME = "table";

    public HiveInput(Config config) {
        super(config);
        
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
