package com.cloudera.labs.envelope.run;

import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.DataFrame;

import com.cloudera.labs.envelope.derive.PassthroughDeriver;
import com.cloudera.labs.envelope.input.batch.BatchInput;
import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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
            deriver = new PassthroughDeriver(ConfigFactory.empty());
            Map<String, DataFrame> dependencies = getStepDataFrames(dependencySteps);
            data = deriver.derive(dependencies);
        }
        
        setData(data);
        
        setFinished(true);
    }

}
