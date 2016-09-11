package com.cloudera.labs.envelope.input.batch;

import org.apache.spark.sql.DataFrame;

import com.cloudera.labs.envelope.input.Input;
import com.typesafe.config.Config;

public abstract class BatchInput extends Input {

    public BatchInput(Config config) {
        super(config);
    }

    public abstract DataFrame read() throws Exception;

}
