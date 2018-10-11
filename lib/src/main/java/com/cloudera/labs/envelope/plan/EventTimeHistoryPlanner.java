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

package com.cloudera.labs.envelope.plan;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.plan.time.TimeModel;
import com.cloudera.labs.envelope.plan.time.TimeModelFactory;
import com.cloudera.labs.envelope.utils.PlannerUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * A planner implementation for storing all versions of the values of a key (its history) using
 * Type II SCD modeling.
 */
public class EventTimeHistoryPlanner
    implements RandomPlanner, ProvidesAlias, ProvidesValidations, InstantiatesComponents {

  public static final String KEY_FIELD_NAMES_CONFIG_NAME = "fields.key";
  public static final String VALUE_FIELD_NAMES_CONFIG_NAME = "fields.values";
  public static final String TIMESTAMP_FIELD_NAMES_CONFIG_NAME = "fields.timestamp";
  public static final String EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME = "fields.effective.from";
  public static final String EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME = "fields.effective.to";
  public static final String CURRENT_FLAG_FIELD_NAME_CONFIG_NAME = "field.current.flag";
  public static final String CURRENT_FLAG_YES_CONFIG_NAME = "current.flag.value.yes";
  public static final String CURRENT_FLAG_NO_CONFIG_NAME = "current.flag.value.no";
  public static final String LAST_UPDATED_FIELD_NAME_CONFIG_NAME = "field.last.updated";
  public static final String CARRY_FORWARD_CONFIG_NAME = "carry.forward.when.null";
  public static final String EVENT_TIME_MODEL_CONFIG_NAME = "time.model.event";
  public static final String LAST_UPDATED_TIME_MODEL_CONFIG_NAME = "time.model.last.updated";

  public static final String CURRENT_FLAG_DEFAULT_YES = "Y";
  public static final String CURRENT_FLAG_DEFAULT_NO = "N";

  private Config config;
  private TimeModel eventTimeModel;
  private TimeModel effectiveFromTimeModel;
  private TimeModel effectiveToTimeModel;
  private TimeModel lastUpdatedTimeModel;

  @Override
  public void configure(Config config) {
    this.config = config;
    
    this.eventTimeModel = getEventTimeModel(true);
    this.effectiveFromTimeModel = getEffectiveFromTimeModel(true);
    this.effectiveToTimeModel = getEffectiveToTimeModel(true);
    if (hasLastUpdatedField()) {
      this.lastUpdatedTimeModel = getLastUpdatedTimeModel(true);
    }
  }

  @Override
  public Set<MutationType> getEmittedMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT, MutationType.UPDATE);
  }

  @Override
  public List<Row> planMutationsForKey(Row key, List<Row> arrivingForKey, List<Row> existingForKey) {
    resetCurrentSystemTime();
    
    List<Row> planned = Lists.newArrayList();
    List<Row> plannedForKey = Lists.newArrayList();

    if (existingForKey != null) {
      for (Row existing : existingForKey) {
        plannedForKey.add(PlannerUtils.appendMutationTypeField(existing));
      }
    }

    Collections.sort(plannedForKey, eventTimeModel);

    for (Row arriving : arrivingForKey) {
      arriving = effectiveFromTimeModel.appendFields(arriving);
      arriving = effectiveToTimeModel.appendFields(arriving);
      if (hasCurrentFlagField()) {
        arriving = RowUtils.append(arriving, getCurrentFlagFieldName(), DataTypes.StringType, null);
      }
      if (hasLastUpdatedField()) {
        arriving = lastUpdatedTimeModel.appendFields(arriving);
      }
      arriving = PlannerUtils.appendMutationTypeField(arriving);

      // There was no existing record for the key, so we just insert the input record.
      if (plannedForKey.size() == 0) {
        arriving = PlannerUtils.copyTime(arriving, eventTimeModel, arriving, effectiveFromTimeModel);
        arriving = effectiveToTimeModel.setFarFutureTime(arriving);
        if (hasCurrentFlagField()) {
          arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), getCurrentFlagYesValue());
        }
        if (hasLastUpdatedField()) {
          arriving = lastUpdatedTimeModel.setCurrentSystemTime(arriving);
        }
        plannedForKey.add(PlannerUtils.setMutationType(arriving, MutationType.INSERT));

        continue;
      }

      // Iterate through each existing record of the key in time order, stopping when we
      // have either corrected the history or gone all the way through it.
      for (int position = 0; position < plannedForKey.size(); position++) {
        Row plan = plannedForKey.get(position);
        Row previousPlanned = null;
        Row nextPlanned = null;

        if (position > 0) {
          previousPlanned = plannedForKey.get(position - 1);
        }
        if (position + 1 < plannedForKey.size()) {
          nextPlanned = plannedForKey.get(position + 1);
        }

        // There is an existing record for the same key and timestamp. It is possible that
        // the existing record is in the storage layer or is about to be added during this
        // micro-batch. Either way, we only update that record if it has changed.
        if (PlannerUtils.simultaneous(eventTimeModel, arriving, plan) &&
            RowUtils.different(arriving, plan, getValueFieldNames()))
        {
          arriving = PlannerUtils.copyTime(plan, effectiveFromTimeModel, arriving, effectiveFromTimeModel);
          arriving = PlannerUtils.copyTime(plan, effectiveToTimeModel, arriving, effectiveToTimeModel);
          if (hasCurrentFlagField()) {
            arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), RowUtils.get(plan, getCurrentFlagFieldName()));
          }
          if (hasLastUpdatedField()) {
            arriving = lastUpdatedTimeModel.setCurrentSystemTime(arriving);
          }

          if (RowUtils.get(plan, MutationType.MUTATION_TYPE_FIELD_NAME).equals(MutationType.INSERT)) {
            plannedForKey.set(position, PlannerUtils.setMutationType(arriving, MutationType.INSERT));
          }
          else {
            plannedForKey.set(position, PlannerUtils.setMutationType(arriving, MutationType.UPDATE));
          }

          break;
        }
        // Before them all
        // -> Insert with ED just before first
        // The input record is timestamped before any existing record of the same key. In
        // this case there is no need to modify existing records, and we only have to insert
        // the input record as effective up until just prior to the first existing record.
        else if (previousPlanned == null && PlannerUtils.before(eventTimeModel, arriving, plan)) {
          arriving = PlannerUtils.copyTime(arriving, eventTimeModel, arriving, effectiveFromTimeModel);
          arriving = PlannerUtils.copyPrecedingTime(plan, eventTimeModel, arriving, effectiveToTimeModel);
          if (hasCurrentFlagField()) {
            arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), getCurrentFlagNoValue());
          }
          if (hasLastUpdatedField()) {
            arriving = lastUpdatedTimeModel.setCurrentSystemTime(arriving);
          }
          plannedForKey.add(PlannerUtils.setMutationType(arriving, MutationType.INSERT));

          break;
        }
        // The input record is timestamped with an existing record of the same key before it
        // and an existing record of the same key after it. We insert the input record
        // effective until just prior to the next existing record and we update the
        // previous existing record to be effective until just prior to the input record.
        else if (plan != null && nextPlanned != null &&
                 PlannerUtils.after(eventTimeModel, arriving, plan) &&
                 PlannerUtils.before(eventTimeModel, arriving, nextPlanned))
        {
          arriving = PlannerUtils.copyTime(arriving, eventTimeModel, arriving, effectiveFromTimeModel);
          arriving = PlannerUtils.copyPrecedingTime(nextPlanned, eventTimeModel, arriving, effectiveToTimeModel);
          if (hasCurrentFlagField()) {
            arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), getCurrentFlagNoValue());
          }
          if (hasLastUpdatedField()) {
            arriving = lastUpdatedTimeModel.setCurrentSystemTime(arriving);
          }
          plannedForKey.add(PlannerUtils.setMutationType(arriving, MutationType.INSERT));

          plan = PlannerUtils.copyPrecedingTime(arriving, eventTimeModel, plan, effectiveToTimeModel);
          
          if (hasCurrentFlagField()) {
            plan = RowUtils.set(plan, getCurrentFlagFieldName(), getCurrentFlagNoValue());
          }
          if (hasLastUpdatedField()) {
            plan = lastUpdatedTimeModel.setCurrentSystemTime(plan);
          }
          if (!PlannerUtils.getMutationType(plan).equals(MutationType.INSERT)) {
            plan = PlannerUtils.setMutationType(plan, MutationType.UPDATE);
          }
          plannedForKey.set(position, plan);

          break;
        }
        // The input record is arriving after all existing records of the same key. This
        // is the 'normal' case where data arrives in order. If the values are different 
        // we insert the input record effective until the far future, and we update the
        // previous existing record to be effective until just prior to the input record.
        else if (PlannerUtils.after(eventTimeModel, arriving, plan) &&
                 RowUtils.different(arriving, plan, getValueFieldNames()) &&
                 nextPlanned == null)
        {
          arriving = PlannerUtils.copyTime(arriving, eventTimeModel, arriving, effectiveFromTimeModel);
          arriving = effectiveToTimeModel.setFarFutureTime(arriving);
          if (hasCurrentFlagField()) {
            arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), getCurrentFlagYesValue());
          }
          if (hasLastUpdatedField()) {
            arriving = lastUpdatedTimeModel.setCurrentSystemTime(arriving);
          }
          plannedForKey.add(PlannerUtils.setMutationType(arriving, MutationType.INSERT));

          plan = PlannerUtils.copyPrecedingTime(arriving, eventTimeModel, plan, effectiveToTimeModel);
          if (hasCurrentFlagField()) {
            plan = RowUtils.set(plan, getCurrentFlagFieldName(), getCurrentFlagNoValue());
          }
          if (hasLastUpdatedField()) {
            plan = lastUpdatedTimeModel.setCurrentSystemTime(plan);
          }
          if (!PlannerUtils.getMutationType(plan).equals(MutationType.INSERT)) {
            plan = PlannerUtils.setMutationType(plan, MutationType.UPDATE);
          }
          plannedForKey.set(position, plan);

          break;
        }
      }

      Collections.sort(plannedForKey, eventTimeModel);
    }

    for (int position = 0; position < plannedForKey.size(); position++) {
      Row plan = plannedForKey.get(position);
      // We carry forward for all mutations in case the next non-NONE row needs the values from this row
      if (position > 0) {
        plan = carryForwardWhenNull(plan, plannedForKey.get(position - 1));
        plannedForKey.set(position, plan);
      }
      if (!PlannerUtils.getMutationType(plan).equals(MutationType.NONE)) {
        planned.add(plan);
      }
    }

    return planned;
  }

  @Override
  public List<String> getKeyFieldNames() {
    return config.getStringList(KEY_FIELD_NAMES_CONFIG_NAME);
  }

  private boolean hasLastUpdatedField() {
    return config.hasPath(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
  }

  private boolean hasCurrentFlagField() {
    return config.hasPath(CURRENT_FLAG_FIELD_NAME_CONFIG_NAME);
  }

  private boolean hasCurrentFlagYes() {
    return config.hasPath(CURRENT_FLAG_YES_CONFIG_NAME);
  }

  private boolean hasCurrentFlagNo() {
    return config.hasPath(CURRENT_FLAG_NO_CONFIG_NAME);
  }

  private String getLastUpdatedFieldName() {
    return config.getString(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
  }

  private String getCurrentFlagFieldName() {
    return config.getString(CURRENT_FLAG_FIELD_NAME_CONFIG_NAME);
  }
  
  private String getCurrentFlagYesValue() {
    return hasCurrentFlagYes() ? config.getString(CURRENT_FLAG_YES_CONFIG_NAME) : CURRENT_FLAG_DEFAULT_YES;
  }

  private String getCurrentFlagNoValue() {
    return hasCurrentFlagNo() ? config.getString(CURRENT_FLAG_NO_CONFIG_NAME) : CURRENT_FLAG_DEFAULT_NO;
  }

  private List<String> getEffectiveToFieldNames() {
    return config.getStringList(EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME);
  }

  private List<String> getEffectiveFromFieldNames() {
    return config.getStringList(EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME);
  }

  private List<String> getValueFieldNames() {
    return config.getStringList(VALUE_FIELD_NAMES_CONFIG_NAME);
  }

  private List<String> getTimestampFieldNames() {
    return config.getStringList(TIMESTAMP_FIELD_NAMES_CONFIG_NAME);
  }

  // When the arrived record value is null then we have the option to carry forward
  // the value from the previous record. This is useful for handling sparse stream records.
  private Row carryForwardWhenNull(Row into, Row from) {
    if (!config.hasPath(CARRY_FORWARD_CONFIG_NAME) || !config.getBoolean(CARRY_FORWARD_CONFIG_NAME)) {
      return into;
    }

    for (StructField field : into.schema().fields()) {
      String fieldName = field.name();
      if (RowUtils.get(into, fieldName) == null && RowUtils.get(from, fieldName) != null) {
        into = RowUtils.set(into, fieldName, RowUtils.get(from, fieldName));
      }
    }

    return into;
  }
  
  private void resetCurrentSystemTime() {
    long currentSystemTimeMillis = System.currentTimeMillis();
    
    this.eventTimeModel.configureCurrentSystemTime(currentSystemTimeMillis);
    this.effectiveFromTimeModel.configureCurrentSystemTime(currentSystemTimeMillis);
    this.effectiveToTimeModel.configureCurrentSystemTime(currentSystemTimeMillis);
    if (hasLastUpdatedField()) {
      this.lastUpdatedTimeModel.configureCurrentSystemTime(currentSystemTimeMillis);
    }
  }
  
  private Config getEventTimeModelConfig() {
    return config.hasPath(EVENT_TIME_MODEL_CONFIG_NAME) ? 
        config.getConfig(EVENT_TIME_MODEL_CONFIG_NAME) : ConfigFactory.empty();
  }
  
  private Config getLastUpdatedTimeModelConfig() {
    return config.hasPath(LAST_UPDATED_TIME_MODEL_CONFIG_NAME) ? 
        config.getConfig(LAST_UPDATED_TIME_MODEL_CONFIG_NAME) : ConfigFactory.empty();
  }

  private TimeModel getEventTimeModel(boolean configure) {
    return TimeModelFactory.create(getEventTimeModelConfig(), getTimestampFieldNames(), configure);
  }

  private TimeModel getEffectiveFromTimeModel(boolean configure) {
    return TimeModelFactory.create(getEventTimeModelConfig(), getEffectiveFromFieldNames(), configure);
  }

  private TimeModel getEffectiveToTimeModel(boolean configure) {
    return TimeModelFactory.create(getEventTimeModelConfig(), getEffectiveToFieldNames(), configure);
  }

  private TimeModel getLastUpdatedTimeModel(boolean configure) {
    return TimeModelFactory.create(getLastUpdatedTimeModelConfig(), getLastUpdatedFieldName(), configure);
  }

  @Override
  public String getAlias() {
    return "history";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(KEY_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(VALUE_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(TIMESTAMP_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .optionalPath(CURRENT_FLAG_FIELD_NAME_CONFIG_NAME, ConfigValueType.STRING)
        .optionalPath(CURRENT_FLAG_YES_CONFIG_NAME, ConfigValueType.STRING)
        .optionalPath(CURRENT_FLAG_NO_CONFIG_NAME, ConfigValueType.STRING)
        .optionalPath(LAST_UPDATED_FIELD_NAME_CONFIG_NAME, ConfigValueType.STRING)
        .optionalPath(CARRY_FORWARD_CONFIG_NAME, ConfigValueType.BOOLEAN)
        .optionalPath(EVENT_TIME_MODEL_CONFIG_NAME, ConfigValueType.OBJECT)
        .optionalPath(LAST_UPDATED_TIME_MODEL_CONFIG_NAME, ConfigValueType.OBJECT)
        .handlesOwnValidationPath(EVENT_TIME_MODEL_CONFIG_NAME)
        .handlesOwnValidationPath(LAST_UPDATED_TIME_MODEL_CONFIG_NAME)
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    this.config = config;

    Set<InstantiatedComponent> components = Sets.newHashSet();

    components.add(new InstantiatedComponent(
        getEventTimeModel(configure), getEventTimeModelConfig(), "Event Time Model"));

    if (hasLastUpdatedField()) {
      components.add(new InstantiatedComponent(getLastUpdatedTimeModel(configure),
          getLastUpdatedTimeModelConfig(), "Last Updated Time Model"));
    }

    return components;
  }

}
