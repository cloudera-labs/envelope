package com.cloudera.labs.envelope.plan.random;

import java.util.List;

import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.plan.PlannedRow;
import com.cloudera.labs.envelope.plan.Planner;
import com.typesafe.config.Config;

public abstract class RandomReadWritePlanner extends Planner {

    public RandomReadWritePlanner(Config config) {
        super(config);
    }
    
    public abstract List<PlannedRow> planMutationsForKey(Row key, List<Row> arrivingForKey, List<Row> existingForKey);
    
    public abstract List<String> getKeyFieldNames();
    
}
