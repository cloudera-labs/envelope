package com.cloudera.fce.envelope.deriver;

import java.util.Properties;

import org.apache.spark.sql.DataFrame;

public class SQLDeriver extends Deriver {

    public SQLDeriver(Properties props) {
        super(props);
    }

    @Override
    public DataFrame derive(DataFrame input) {
        String query = props.getProperty("query.literal");
        
        return input.sqlContext().sql(query);
    }

}
