package com.cloudera.labs.envelope.plan.random;

import java.util.List;

import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.plan.PlannedRow;
import com.cloudera.labs.envelope.plan.Planner;
import com.typesafe.config.Config;

public abstract class RandomWritePlanner extends Planner {

    public RandomWritePlanner(Config config) {
        super(config);
    }
    
    public abstract List<PlannedRow> planMutationsForRow(Row arriving);
    
}
