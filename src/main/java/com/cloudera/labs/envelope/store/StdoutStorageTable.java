package com.cloudera.labs.envelope.store;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRecord;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 *
 */
public class StdoutStorageTable extends StorageTable {

  private String prefix;

  public StdoutStorageTable(String prefix) {
    this.prefix = prefix;
  }

  /**
   * The mutation types that the storage table is able to apply.
   *
   * @return The set of supported mutation types.
   */
  @Override
  public Set<MutationType> getSupportedMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT, MutationType.UPDATE, MutationType.DELETE, MutationType.NONE);
  }

  /**
   * The schema of the storage table.
   *
   * @return The Apache Avro schema equivalent for the schema of the storage table.
   */
  @Override
  public Schema getSchema() {
    return null;
  }

  /**
   * Get the existing records in the storage table for the given filter.
   *
   * @param filter The record whose field names and values are used as an equality filter on the storage table.
   * @return The list of storage records in the table schema that match the filter.
   */
  @Override
  public List<GenericRecord> getExistingForFilter(GenericRecord filter) throws Exception {
    return null;
  }

  /**
   * Apply the planned mutations to the storage table.
   *
   * @param mutations The list of planned mutations.
   */
  @Override
  public void applyPlannedMutations(List<PlannedRecord> mutations) throws Exception {
    for (PlannedRecord record : mutations) {
      System.out.println(prefix + " :: " + record);
    }
  }
}
