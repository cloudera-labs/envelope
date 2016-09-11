package com.cloudera.labs.envelope.output.random;

import org.apache.spark.sql.Row;

import com.typesafe.config.Config;

public abstract class RandomReadWriteOutput extends RandomWriteOutput {

    public RandomReadWriteOutput(Config config) {
        super(config);
    }
    
    public abstract Iterable<Row> getExistingForFilters(Iterable<Row> filters) throws Exception;
    
}
