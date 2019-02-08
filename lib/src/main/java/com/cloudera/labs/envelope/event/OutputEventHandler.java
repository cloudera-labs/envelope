/*
 * Copyright (c) 2015-2019, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.labs.envelope.event;

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.output.Output;
import com.cloudera.labs.envelope.output.OutputFactory;
import com.cloudera.labs.envelope.output.RandomOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.PlannerUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validation;
import com.cloudera.labs.envelope.validate.ValidationResult;
import com.cloudera.labs.envelope.validate.Validations;
import com.cloudera.labs.envelope.validate.Validity;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Set;

public class OutputEventHandler implements EventHandler, ProvidesAlias, ProvidesValidations,
    InstantiatesComponents {

  private RandomOutput randomOutput;
  private StructType schema;
  private Set<String> handledEventTypes;

  public static final String LOG_ALL_EVENTS_CONFIG = "log-all-events";
  public static final String OUTPUT_CONFIG = "output";

  public static final String EVENT_ID_FIELD = "event_id";
  public static final String TIMESTAMP_UTC_FIELD = "timestamp_utc";
  public static final String EVENT_TYPE_FIELD = "event_type";
  public static final String MESSAGE_FIELD = "message";
  public static final String PIPELINE_ID_FIELD = "pipeline_id";
  public static final String APPLICATION_ID_FIELD = "application_id";

  @Override
  public void configure(Config config) {
    Config outputConfig = config.getConfig(OUTPUT_CONFIG);
    this.randomOutput = (RandomOutput)OutputFactory.create(outputConfig, true);

    boolean allEventsEnabled = ConfigUtils.getOrElse(config, LOG_ALL_EVENTS_CONFIG, false);
    handledEventTypes = CoreEventTypes.getAllCoreEventTypes();
    if (!allEventsEnabled) {
      handledEventTypes.removeAll(CoreEventTypes.getHighPerformanceImpactCoreEventTypes());
    }
  }

  @Override
  public void handle(Event event) {
    Row mutation = new RowWithSchema(getSchema(),
        event.getId(),
        event.getTimestamp(),
        event.getEventType(),
        event.getMessage(),
        Contexts.getPipelineID(),
        Contexts.getApplicationID());
    mutation = PlannerUtils.setMutationType(mutation, MutationType.INSERT);
    List<Row> mutations = Lists.newArrayList(mutation);

    try {
      randomOutput.applyRandomMutations(mutations);
    } catch (Exception e) {
      throw new RuntimeException("Could not write Envelope event to output", e);
    }
  }

  private StructType getSchema() {
    if (schema == null) {
      schema = DataTypes.createStructType(Lists.newArrayList(
          DataTypes.createStructField(EVENT_ID_FIELD, DataTypes.StringType, false),
          DataTypes.createStructField(TIMESTAMP_UTC_FIELD, DataTypes.TimestampType, false),
          DataTypes.createStructField(EVENT_TYPE_FIELD, DataTypes.StringType, false),
          DataTypes.createStructField(MESSAGE_FIELD, DataTypes.StringType, false),
          DataTypes.createStructField(PIPELINE_ID_FIELD, DataTypes.StringType, false),
          DataTypes.createStructField(APPLICATION_ID_FIELD, DataTypes.StringType, false)
      ));
    }

    return schema;
  }

  @Override
  public boolean canHandleEventType(String eventType) {
    return handledEventTypes.contains(eventType);
  }

  @Override
  public String getAlias() {
    return "output";
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    return Sets.newHashSet(new InstantiatedComponent(randomOutput, config, "Output"));
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(OUTPUT_CONFIG, ConfigValueType.OBJECT)
        .optionalPath(LOG_ALL_EVENTS_CONFIG, ConfigValueType.BOOLEAN)
        .handlesOwnValidationPath(OUTPUT_CONFIG)
        .add(new CompatibleOutputValidation())
        .build();
  }

  private static class CompatibleOutputValidation implements Validation {
    @Override
    public ValidationResult validate(Config config) {
      Config outputConfig = config.getConfig(OUTPUT_CONFIG);
      Output output = OutputFactory.create(outputConfig, false);

      if (!(output instanceof RandomOutput)) {
        return new ValidationResult(
            this, Validity.INVALID, "Output event handler output is not a random output");
      }

      RandomOutput randomOutput = (RandomOutput)output;

      if (randomOutput.getSupportedRandomMutationTypes().contains(MutationType.INSERT)) {
        return new ValidationResult(
            this, Validity.VALID,
            "Output event handler output is a random output that supports inserts");
      }
      else {
        return new ValidationResult(
            this, Validity.INVALID,
            "Output event handler output is a random output but does not support inserts");
      }
    }

    @Override
    public Set<String> getKnownPaths() {
      return Sets.newHashSet(OUTPUT_CONFIG);
    }
  }

}
