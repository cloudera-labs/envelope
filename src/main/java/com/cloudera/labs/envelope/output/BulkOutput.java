package com.cloudera.labs.envelope.output;

import java.util.List;
import java.util.Set;

import org.apache.spark.sql.DataFrame;

import com.cloudera.labs.envelope.plan.MutationType;

import scala.Tuple2;

public interface BulkOutput extends Output {
    
    Set<MutationType> getSupportedBulkMutationTypes();

    void applyBulkMutations(List<Tuple2<MutationType, DataFrame>> planned) throws Exception;

}
