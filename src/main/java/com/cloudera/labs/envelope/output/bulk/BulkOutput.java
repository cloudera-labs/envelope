package com.cloudera.labs.envelope.output.bulk;

import java.util.List;

import org.apache.spark.sql.DataFrame;

import com.cloudera.labs.envelope.output.Output;
import com.cloudera.labs.envelope.plan.MutationType;
import com.typesafe.config.Config;

import scala.Tuple2;

public abstract class BulkOutput extends Output {

    public BulkOutput(Config config) {
        super(config);
    }
    
    public abstract void applyMutations(List<Tuple2<MutationType, DataFrame>> planned) throws Exception;

}
