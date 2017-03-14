package com.cloudera.labs.envelope.output;

import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;

public interface RandomOutput extends Output {

  Set<MutationType> getSupportedRandomMutationTypes();

  void applyRandomMutations(List<PlannedRow> planned) throws Exception;

  Iterable<Row> getExistingForFilters(Iterable<Row> filters) throws Exception;

}
