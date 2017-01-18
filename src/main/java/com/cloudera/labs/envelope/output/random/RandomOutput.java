package com.cloudera.labs.envelope.output.random;

import java.util.List;

import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.output.Output;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.typesafe.config.Config;

public abstract class RandomOutput extends Output {

    public RandomOutput(Config config) {
        super(config);
    }
    
    public abstract void applyMutations(List<PlannedRow> planned) throws Exception;
    
    public abstract Iterable<Row> getExistingForFilters(Iterable<Row> filters) throws Exception;
    
}
