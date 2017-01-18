package com.cloudera.labs.envelope.plan.bulk;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.DataFrame;

import com.cloudera.labs.envelope.plan.MutationType;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

public class OverwritePlanner extends BulkPlanner {

    public OverwritePlanner(Config config) {
        super(config);
    }

    @Override
    public List<Tuple2<MutationType, DataFrame>> planMutationsForSet(DataFrame arriving) {
        Tuple2<MutationType, DataFrame> mutation = new Tuple2<>(MutationType.OVERWRITE, arriving);
        
        List<Tuple2<MutationType, DataFrame>> mutations = new ArrayList<>();
        mutations.add(mutation);
        
        return mutations;
    }

    @Override
    public Set<MutationType> getEmittedMutationTypes() {
        return Sets.newHashSet(MutationType.OVERWRITE);
    }

}
