/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.run;

import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.derive.PassthroughDeriver;
import com.cloudera.labs.envelope.input.BatchInput;
import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;

/**
 * A batch step is a data step that contains a single DataFrame.
 */
public class BatchStep extends DataStep {
  
  public static final String REPARTITION_NUM_PARTITIONS_PROPERTY = "repartition.partitions";
  public static final String COALESCE_NUM_PARTITIONS_PROPERTY = "coalesce.partitions";

  private static final String INPUT_PREFIX = "input.";
  private static final String DERIVER_PREFIX = "deriver.";
  
  public BatchStep(String name, Config config) throws Exception {
    super(name, config);
    
    if ((config.hasPath(INPUT_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY) ||
         config.hasPath(DERIVER_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY)) &&
        (config.hasPath(INPUT_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY) ||
         config.hasPath(DERIVER_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY)))
    {
      throw new RuntimeException("Step " + getName() + " can not both repartition and coalesce.");
    }
  }

  public void runStep(Set<Step> dependencySteps) throws Exception {
    Contexts.getSparkSession().sparkContext().setJobDescription("Step: " + getName());

    Dataset<Row> data;
    if (hasInput()) {
      data = ((BatchInput)input).read();
    }
    else if (hasDeriver()) {
      Map<String, Dataset<Row>> dependencies = getStepDataFrames(dependencySteps);
      data = deriver.derive(dependencies);
    }
    else {
      deriver = new PassthroughDeriver();
      Map<String, Dataset<Row>> dependencies = getStepDataFrames(dependencySteps);
      data = deriver.derive(dependencies);
    }
    
    if (doesRepartition()) {
      data = repartition(data);
    }

    setData(data);

    setFinished(true);
  }
  
  private boolean doesRepartition() {
    return config.hasPath(INPUT_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY) ||
           config.hasPath(DERIVER_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY) ||
           config.hasPath(INPUT_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY) ||
           config.hasPath(DERIVER_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY);
  }

  private Dataset<Row> repartition(Dataset<Row> data) {
    if (config.hasPath(INPUT_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY)) {
      int numPartitions = config.getInt(INPUT_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY);
      data = data.repartition(numPartitions);
    }
    else if (config.hasPath(DERIVER_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY)) {
      int numPartitions = config.getInt(DERIVER_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY);
      data = data.repartition(numPartitions);
    }
    else if (config.hasPath(INPUT_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY)) {
      int numPartitions = config.getInt(INPUT_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY);
      data = data.coalesce(numPartitions);
    }
    else if (config.hasPath(DERIVER_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY)) {
      int numPartitions = config.getInt(DERIVER_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY);
      data = data.coalesce(numPartitions);
    }
    
    return data;
  }

}
