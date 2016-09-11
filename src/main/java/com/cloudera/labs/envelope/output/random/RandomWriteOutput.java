package com.cloudera.labs.envelope.output.random;

import java.util.List;

import com.cloudera.labs.envelope.output.Output;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.typesafe.config.Config;

public abstract class RandomWriteOutput extends Output {

    public RandomWriteOutput(Config config) {
        super(config);
    }

    public abstract void applyMutations(List<PlannedRow> planned) throws Exception;
    
}
