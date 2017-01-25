package com.cloudera.labs.envelope.plan;

import java.util.List;

import org.apache.spark.sql.DataFrame;

import scala.Tuple2;

public interface BulkPlanner extends Planner {
    
    List<Tuple2<MutationType, DataFrame>> planMutationsForSet(DataFrame arriving);

}
