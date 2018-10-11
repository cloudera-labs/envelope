/*
 * Copyright (c) 2015-2018, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.labs.envelope.run;

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.input.BatchInput;
import com.cloudera.labs.envelope.repetition.Repetition;
import com.cloudera.labs.envelope.repetition.RepetitionFactory;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.StepUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validation;
import com.cloudera.labs.envelope.validate.ValidationResult;
import com.cloudera.labs.envelope.validate.Validations;
import com.cloudera.labs.envelope.validate.Validity;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A batch step is a data step that contains a single DataFrame.
 */
public class BatchStep extends DataStep implements ProvidesValidations, InstantiatesComponents {
  
  public static final String REPARTITION_NUM_PARTITIONS_PROPERTY = "repartition.partitions";
  public static final String REPARTITION_COLUMNS_PROPERTY = "repartition.columns";
  public static final String COALESCE_NUM_PARTITIONS_PROPERTY = "coalesce.partitions";

  private static final String REPETITION_PREFIX = "repetitions";
  
  public BatchStep(String name) {
    super(name);
  }

  @Override
  public void configure(Config config) {
    super.configure(config);

    createRepetitions(config, true);
  }

  public void submit(Set<Step> dependencySteps) throws Exception {
    Contexts.getSparkSession().sparkContext().setJobDescription("Step: " + getName());

    Dataset<Row> data;
    if (hasInput()) {
      data = ((BatchInput)getInput(true)).read();
    }
    else if (hasDeriver()) {
      Map<String, Dataset<Row>> dependencies = StepUtils.getStepDataFrames(dependencySteps);
      data = getDeriver(true).derive(dependencies);
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
    return config.hasPath(REPARTITION_NUM_PARTITIONS_PROPERTY) ||
           config.hasPath(REPARTITION_COLUMNS_PROPERTY) ||
           config.hasPath(COALESCE_NUM_PARTITIONS_PROPERTY);
  }

  private Dataset<Row> repartition(Dataset<Row> data) {
    int numPartitions = 0;
    List<String> colPartitions = null;

    if (config.hasPath(REPARTITION_NUM_PARTITIONS_PROPERTY)) {
      numPartitions = config.getInt(REPARTITION_NUM_PARTITIONS_PROPERTY);
    }

    if (config.hasPath(REPARTITION_COLUMNS_PROPERTY)) {
      colPartitions = config.getStringList(REPARTITION_COLUMNS_PROPERTY);
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

    if (config.hasPath(COALESCE_NUM_PARTITIONS_PROPERTY)) {
      numPartitions = config.getInt(COALESCE_NUM_PARTITIONS_PROPERTY);
      data = data.coalesce(numPartitions);
    }
    
    return data;
  }

  private Map<String, Repetition> createRepetitions(Config config, boolean configure) {
    Map<String, Repetition> repetitions = Maps.newHashMap();

    if (config.hasPath(REPETITION_PREFIX)) {
      Config repConfig = config.getConfig(REPETITION_PREFIX);
      for (String rep : repConfig.root().keySet()) {
        Repetition repetition = RepetitionFactory.create(
            this, rep, repConfig.getConfig(rep), configure);
        repetitions.put(rep, repetition);
      }
    }

    return repetitions;
  }

  @Override
  public Step copy() {
    BatchStep copy = new BatchStep(name);
    copy.configure(config);
    
    copy.setSubmitted(hasSubmitted());
    
    if (hasSubmitted()) {
      copy.setData(getData());
    }
    
    return copy;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .add(new RepartitionValidation())
        .optionalPath(REPETITION_PREFIX, ConfigValueType.OBJECT)
        .handlesOwnValidationPath(REPETITION_PREFIX)
        .addAll(super.getValidations())
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    Set<InstantiatedComponent> components = Sets.newHashSet();

    components.addAll(super.getComponents(config, configure));

    Map<String, Repetition> repetitions = createRepetitions(config, configure);

    if (config.hasPath(REPETITION_PREFIX)) {
      Config repConfig = config.getConfig(REPETITION_PREFIX);
      for (String repetitionName : repConfig.root().keySet()) {
        Repetition repetition = repetitions.get(repetitionName);
        components.add(new InstantiatedComponent(
            repetition, repConfig.getConfig(repetitionName), "Repetition"));
      }
    }

    return components;
  }

  private static class RepartitionValidation implements Validation {
    @Override
    public ValidationResult validate(Config config) {
      if ((config.hasPath(REPARTITION_NUM_PARTITIONS_PROPERTY) ||
          config.hasPath(REPARTITION_COLUMNS_PROPERTY)) &&
          config.hasPath(COALESCE_NUM_PARTITIONS_PROPERTY))
      {
        return new ValidationResult(this, Validity.INVALID, "Can not both repartition and coalesce");
      }
      else {
        return new ValidationResult(this, Validity.VALID, "Does not both repartition and coalesce");
      }
    }

    @Override
    public Set<String> getKnownPaths() {
      return Sets.newHashSet(
          REPARTITION_NUM_PARTITIONS_PROPERTY,
          REPARTITION_COLUMNS_PROPERTY,
          COALESCE_NUM_PARTITIONS_PROPERTY);
    }
  }

}
