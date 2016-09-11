package com.cloudera.labs.envelope.input.batch;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;

public class KuduInput extends BatchInput {
    
    public static final String CONNECTION_CONFIG_NAME = "connection";
    public static final String TABLE_NAME_CONFIG_NAME = "table.name";
    
    public KuduInput(Config config) {
        super(config);
    }
    
    @Override
    public DataFrame read() throws Exception {
        String connection = config.getString(CONNECTION_CONFIG_NAME);
        String tableName = config.getString(TABLE_NAME_CONFIG_NAME);
        
        SQLContext sqlc = Contexts.getSQLContext();
        DataFrame tableDF = sqlc.read()
                                .format("org.apache.kudu.spark.kudu")
                                .option("kudu.master", connection)
                                .option("kudu.table", tableName)
                                .load();
        
        return tableDF;
    }

}
