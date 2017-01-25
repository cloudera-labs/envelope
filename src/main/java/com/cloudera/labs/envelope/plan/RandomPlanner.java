package com.cloudera.labs.envelope.plan;

import java.util.List;

import org.apache.spark.sql.Row;

public interface RandomPlanner extends Planner {

    List<PlannedRow> planMutationsForKey(Row key, List<Row> arrivingForKey, List<Row> existingForKey);
    
    List<String> getKeyFieldNames();
    
}
