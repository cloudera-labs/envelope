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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.input.BatchInput;
import com.cloudera.labs.envelope.repetition.RepetitionFactory;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.StepUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;

/**
 * A batch step is a data step that contains a single DataFrame.
 */
public class BatchStep extends DataStep {
  
  public static final String REPARTITION_NUM_PARTITIONS_PROPERTY = "repartition.partitions";
  public static final String REPARTITION_COLUMNS_PROPERTY = "repartition.columns";
  public static final String COALESCE_NUM_PARTITIONS_PROPERTY = "coalesce.partitions";

  private static final String INPUT_PREFIX = "input.";
  private static final String DERIVER_PREFIX = "deriver.";
  private static final String REPETITION_PREFIX = "repetitions";
  
  public BatchStep(String name, Config config) {
    super(name, config);
    
    if ((config.hasPath(INPUT_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY) ||
         config.hasPath(DERIVER_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY) ||
         config.hasPath(INPUT_PREFIX + REPARTITION_COLUMNS_PROPERTY) ||
         config.hasPath(DERIVER_PREFIX + REPARTITION_COLUMNS_PROPERTY)) &&
        (config.hasPath(INPUT_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY) ||
         config.hasPath(DERIVER_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY)))
    {
      throw new RuntimeException("Step " + getName() + " can not both repartition and coalesce.");
    }

    if (config.hasPath(REPETITION_PREFIX)) {
      ConfigObject repConfig = config.getObject(REPETITION_PREFIX);
      for (String rep : repConfig.keySet()) {
        RepetitionFactory.create(this, rep, config.getConfig(REPETITION_PREFIX).getConfig(rep));
      }
    }
  }

  public void submit(Set<Step> dependencySteps) throws Exception {
    Contexts.getSparkSession().sparkContext().setJobDescription("Step: " + getName());

    Dataset<Row> data;
    if (hasInput()) {
      data = ((BatchInput)getInput()).read();
    }
    else if (hasDeriver()) {
      Map<String, Dataset<Row>> dependencies = StepUtils.getStepDataFrames(dependencySteps);
      data = getDeriver().derive(dependencies);
    }
    else {
      throw new RuntimeException("Batch step '" + getName() + "' must contain either an input or a deriver.");
    }
    
    if (doesRepartition()) {
      data = repartition(data);
    }

    setData(data);

    setSubmitted(true);
  }
  
  private boolean doesRepartition() {
    return config.hasPath(INPUT_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY) ||
           config.hasPath(DERIVER_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY) ||
           config.hasPath(INPUT_PREFIX + REPARTITION_COLUMNS_PROPERTY) ||
           config.hasPath(DERIVER_PREFIX + REPARTITION_COLUMNS_PROPERTY) ||
           config.hasPath(INPUT_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY) ||
           config.hasPath(DERIVER_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY);
  }

  private Dataset<Row> repartition(Dataset<Row> data) {
    int numPartitions = 0;
    List<String> colPartitions = null;

    if (config.hasPath(INPUT_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY)) {
      numPartitions = config.getInt(INPUT_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY);
    }
    else if (config.hasPath(DERIVER_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY)) {
      numPartitions = config.getInt(DERIVER_PREFIX + REPARTITION_NUM_PARTITIONS_PROPERTY);
    }

    if (config.hasPath(INPUT_PREFIX + REPARTITION_COLUMNS_PROPERTY)) {
      colPartitions = config.getStringList(INPUT_PREFIX + REPARTITION_COLUMNS_PROPERTY);
    }
    else if (config.hasPath(DERIVER_PREFIX + REPARTITION_COLUMNS_PROPERTY)) {
      colPartitions = config.getStringList(DERIVER_PREFIX + REPARTITION_COLUMNS_PROPERTY);
    }

    if (numPartitions > 0 && null != colPartitions) {
      data = data.repartition(numPartitions, RowUtils.toColumnArray(colPartitions));
    }
    else if (numPartitions > 0) {
      data = data.repartition(numPartitions);
    }
    else if (null != colPartitions) {
      data = data.repartition(RowUtils.toColumnArray(colPartitions));
    }

    if (config.hasPath(INPUT_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY)) {
      numPartitions = config.getInt(INPUT_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY);
      data = data.coalesce(numPartitions);
    }
    else if (config.hasPath(DERIVER_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY)) {
      numPartitions = config.getInt(DERIVER_PREFIX + COALESCE_NUM_PARTITIONS_PROPERTY);
      data = data.coalesce(numPartitions);
    }
    
    return data;
  }
  
  @Override
  public Step copy() {
    BatchStep copy = new BatchStep(name, config);
    
    copy.setSubmitted(hasSubmitted());
    
    if (hasSubmitted()) {
      copy.setData(getData());
    }
    
    return copy;
  }
}
