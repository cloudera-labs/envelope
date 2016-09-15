package com.cloudera.labs.envelope.store;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRecord;
import java.io.File;
import java.util.Arrays;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/**
 *
 */
public class StdoutStorageSystemTest {

  private static final String SCHEMA_FILE = "/morphline-translator.avro";

  private String getResourcePath(String resource) {
    return StdoutStorageSystemTest.class.getResource(resource).getPath();
  }

  @Test
  public void tableFor() throws Exception {
    Properties props = new Properties();
    props.setProperty("storage.table.name", "TEST");

    StdoutStorageSystem storageSystem = new StdoutStorageSystem(props);
    StorageTable table = storageSystem.tableFor(props);

    Schema schema = new Schema.Parser().parse(new File(getResourcePath(SCHEMA_FILE)));
    GenericRecord record = new GenericData.Record(schema);
    record.put("foo", "test");
    record.put("bar", 123);

    table.applyPlannedMutations(Arrays.asList(new PlannedRecord(record, MutationType.INSERT)));
  }

}