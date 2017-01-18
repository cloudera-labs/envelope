package com.cloudera.labs.envelope.plan.bulk;

import java.util.List;

import org.apache.spark.sql.DataFrame;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.Planner;
import com.typesafe.config.Config;

import scala.Tuple2;

public abstract class BulkPlanner extends Planner {

    public BulkPlanner(Config config) {
        super(config);
    }
    
    public abstract List<Tuple2<MutationType, DataFrame>> planMutationsForSet(DataFrame arriving);

}
