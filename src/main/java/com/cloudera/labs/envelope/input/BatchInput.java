package com.cloudera.labs.envelope.input;

import org.apache.spark.sql.DataFrame;

public interface BatchInput extends Input {

  DataFrame read() throws Exception;

}
