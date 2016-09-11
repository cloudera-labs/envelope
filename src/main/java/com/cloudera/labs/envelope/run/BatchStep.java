package com.cloudera.labs.envelope.run;

import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.DataFrame;

import com.cloudera.labs.envelope.input.batch.BatchInput;
import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;

public class BatchStep extends DataStep {

    public BatchStep(String name, Config config) throws Exception {
        super(name, config);
    }
    
    public void runStep(Set<Step> dependencySteps) throws Exception {
        Contexts.getJavaSparkContext().sc().setJobDescription("Step: " + getName());
        
        DataFrame data;
        if (hasInput()) {
            data = ((BatchInput)input).read();
        }
        else if (hasDeriver()) {
            Map<String, DataFrame> dependencies = getStepDataFrames(dependencySteps);
            data = deriver.derive(dependencies);
        }
        else {
            throw new RuntimeException("Batch steps must have either an input or a deriver");
        }
        
        setData(data);
        
        setFinished(true);
    }

}
