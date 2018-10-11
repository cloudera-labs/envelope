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
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.ConfigUtils;
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
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class BitemporalHistoryPlanner implements RandomPlanner, ProvidesAlias, ProvidesValidations, InstantiatesComponents {

  public static final String KEY_FIELD_NAMES_CONFIG_NAME = "fields.key";
  public static final String VALUE_FIELD_NAMES_CONFIG_NAME = "fields.values";
  public static final String TIMESTAMP_FIELD_NAMES_CONFIG_NAME = "fields.timestamp";
  public static final String EVENT_TIME_EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME = "fields.event.time.effective.from";
  public static final String EVENT_TIME_EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME = "fields.event.time.effective.to";
  public static final String SYSTEM_TIME_EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME = "fields.system.time.effective.from";
  public static final String SYSTEM_TIME_EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME = "fields.system.time.effective.to";
  public static final String CURRENT_FLAG_FIELD_NAME_CONFIG_NAME = "field.current.flag";
  public static final String CURRENT_FLAG_YES_CONFIG_NAME = "current.flag.value.yes";
  public static final String CURRENT_FLAG_NO_CONFIG_NAME = "current.flag.value.no";
  public static final String CARRY_FORWARD_CONFIG_NAME = "carry.forward.when.null";
  public static final String EVENT_TIME_MODEL_CONFIG_NAME = "time.model.event";
  public static final String SYSTEM_TIME_MODEL_CONFIG_NAME = "time.model.system";

  public static final String CURRENT_FLAG_DEFAULT_YES = "Y";
  public static final String CURRENT_FLAG_DEFAULT_NO = "N";
  
  private Config config;
  private Row currentSystemTimeRow;
  private TimeModel timestampTimeModel;
  private TimeModel eventEffectiveFromTimeModel;
  private TimeModel eventEffectiveToTimeModel;
  private TimeModel systemEffectiveFromTimeModel;
  private TimeModel systemEffectiveToTimeModel;

  @Override
  public void configure(Config config) {
    this.config = config;

    this.timestampTimeModel = getTimestampTimeModel(true);
    this.eventEffectiveFromTimeModel = getEventEffectiveFromTimeModel(true);
    this.eventEffectiveToTimeModel = getEventEffectiveToTimeModel(true);
    this.systemEffectiveFromTimeModel = getSystemEffectiveFromTimeModel(true);
    this.systemEffectiveToTimeModel = getSystemEffectiveToTimeModel(true);
  }

  @Override
  public Set<MutationType> getEmittedMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT, MutationType.UPDATE);
  }

  @Override
  public List<Row> planMutationsForKey(Row key, List<Row> arrivingForKey, List<Row> existingForKey) {
    resetCurrentSystemTime();
    
    List<Row> plannedForKey = Lists.newArrayList();

    // Filter out existing entries for this key that have already been closed
    if (existingForKey != null) {
      for (Row existing : existingForKey) {
        if (PlannerUtils.before(systemEffectiveToTimeModel, currentSystemTimeRow, existing)) {
          existing = PlannerUtils.appendMutationTypeField(existing);
          plannedForKey.add(existing);
        }
      }
    }

    Collections.sort(plannedForKey, timestampTimeModel);
    Collections.sort(arrivingForKey, timestampTimeModel);

    for (Row arriving : arrivingForKey) {
      arriving = eventEffectiveFromTimeModel.appendFields(arriving);
      arriving = eventEffectiveToTimeModel.appendFields(arriving);
      arriving = systemEffectiveFromTimeModel.appendFields(arriving);
      arriving = systemEffectiveToTimeModel.appendFields(arriving);
      if (hasCurrentFlagField()) {
        arriving = RowUtils.append(arriving, getCurrentFlagFieldName(), DataTypes.StringType, null);
      }
      arriving = PlannerUtils.appendMutationTypeField(arriving);

      // There was no existing record for the key, so we just insert the input record.
      if (plannedForKey.isEmpty()) {
        arriving = PlannerUtils.copyTime(arriving, timestampTimeModel, arriving, eventEffectiveFromTimeModel);
        arriving = eventEffectiveToTimeModel.setFarFutureTime(arriving);
        arriving = systemEffectiveFromTimeModel.setCurrentSystemTime(arriving);
        arriving = systemEffectiveToTimeModel.setFarFutureTime(arriving);
        if (hasCurrentFlagField()) {
          arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), getCurrentFlagYesValue());
        }
        arriving = PlannerUtils.setMutationType(arriving, MutationType.INSERT);
        plannedForKey.add(arriving);

        continue;
      }

      // Iterate through each existing record of the key in time order, stopping when we
      // have either corrected the history or gone all the way through it.
      for (int position = 0; position < plannedForKey.size(); position++) {
        Row plan = plannedForKey.get(position);
        Row previousPlanned = position > 0 ? plannedForKey.get(position - 1) : null;
        Row nextPlanned = position + 1 < plannedForKey.size() ? plannedForKey.get(position + 1) : null;

        // There is an existing record for the same key and timestamp. It is possible that
        // the existing record is in the storage layer or is about to be added during this
        // micro-batch. Either way, we only update that record if it has changed.
        if (PlannerUtils.simultaneous(timestampTimeModel, arriving, plan) && RowUtils.different(arriving, plan, getValueFieldNames())) {
          arriving = PlannerUtils.copyTime(plan, eventEffectiveFromTimeModel, arriving, eventEffectiveFromTimeModel);
          arriving = PlannerUtils.copyTime(plan, eventEffectiveToTimeModel, arriving, eventEffectiveToTimeModel);
          arriving = systemEffectiveFromTimeModel.setCurrentSystemTime(arriving);
          arriving = systemEffectiveToTimeModel.setFarFutureTime(arriving);
          if (hasCurrentFlagField()) {
            arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), RowUtils.get(plan, getCurrentFlagFieldName()));
          }
          arriving = PlannerUtils.setMutationType(arriving, MutationType.INSERT);
          plannedForKey.add(arriving);

          plan = systemEffectiveToTimeModel.setPrecedingSystemTime(plan);
          if (hasCurrentFlagField()) {
            plan = RowUtils.set(plan, getCurrentFlagFieldName(), getCurrentFlagNoValue());
          }
          if (!PlannerUtils.getMutationType(plan).equals(MutationType.INSERT)) {
            plan = PlannerUtils.setMutationType(plan, MutationType.UPDATE);
          }
          plannedForKey.set(position, plan);

          break;
        }
        // Before them all
        // -> Insert with ED just before first
        // The input record is timestamped before any existing record of the same key. In
        // this case there is no need to modify existing records, and we only have to insert
        // the input record as effective up until just prior to the first existing record.
        else if (previousPlanned == null && PlannerUtils.before(timestampTimeModel, arriving, plan)) {
          arriving = PlannerUtils.copyTime(arriving, timestampTimeModel, arriving, eventEffectiveFromTimeModel);
          arriving = PlannerUtils.copyPrecedingTime(plan, timestampTimeModel, arriving, eventEffectiveToTimeModel);
          arriving = systemEffectiveFromTimeModel.setCurrentSystemTime(arriving);
          arriving = systemEffectiveToTimeModel.setFarFutureTime(arriving);
          if (hasCurrentFlagField()) {
            arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), getCurrentFlagNoValue());
          }
          arriving = PlannerUtils.setMutationType(arriving, MutationType.INSERT);
          plannedForKey.add(arriving);

          break;
        }
        // The input record is timestamped with an existing record of the same key before it
        // and an existing record of the same key after it. We insert the input record
        // effective until just prior to the next existing record and we update the
        // previous existing record to be effective until just prior to the input record.
        else if (plan != null && nextPlanned != null &&
                 PlannerUtils.after(timestampTimeModel, arriving, plan) &&
                 PlannerUtils.before(timestampTimeModel, arriving, nextPlanned))
        {
          arriving = PlannerUtils.copyTime(arriving, timestampTimeModel, arriving, eventEffectiveFromTimeModel);
          arriving = PlannerUtils.copyPrecedingTime(nextPlanned, timestampTimeModel, arriving, eventEffectiveToTimeModel);
          arriving = systemEffectiveFromTimeModel.setCurrentSystemTime(arriving);
          arriving = systemEffectiveToTimeModel.setFarFutureTime(arriving);
          if (hasCurrentFlagField()) {
            arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), getCurrentFlagNoValue());
          }
          arriving = PlannerUtils.setMutationType(arriving, MutationType.INSERT);
          plannedForKey.add(arriving);
          
          // We only need to supersede a record that has already become visible in the output.
          // We know this has happened if the record in the plan has an earlier system
          // time than the current system time.
          if (PlannerUtils.before(systemEffectiveFromTimeModel, plan, currentSystemTimeRow)) {
            plan = systemEffectiveToTimeModel.setPrecedingSystemTime(plan);
            if (hasCurrentFlagField()) {
              plan = RowUtils.set(plan, getCurrentFlagFieldName(), getCurrentFlagNoValue());
            }
            if (!PlannerUtils.getMutationType(plan).equals(MutationType.INSERT)) {
              plan = PlannerUtils.setMutationType(plan, MutationType.UPDATE);
            }
            plannedForKey.set(position, plan);
            
            plan = PlannerUtils.copyPrecedingTime(arriving, timestampTimeModel, plan, eventEffectiveToTimeModel);
            plan = systemEffectiveFromTimeModel.setCurrentSystemTime(plan);
            plan = systemEffectiveToTimeModel.setFarFutureTime(plan);
            plan = PlannerUtils.setMutationType(plan, MutationType.INSERT);
            plannedForKey.add(plan);
          }
          else {
            plan = PlannerUtils.copyPrecedingTime(arriving, timestampTimeModel, plan, eventEffectiveToTimeModel);
            plannedForKey.set(position, plan);
          }
          
          break;
        }
        // The input record is arriving after all existing records of the same key. This
        // is the 'normal' case where data arrives in order. If the values are different 
        // we insert the input record effective until the far future, and we update the
        // previous existing record to be effective until just prior to the input record.
        else if (PlannerUtils.after(timestampTimeModel, arriving, plan) &&
                 RowUtils.different(arriving, plan, getValueFieldNames()) &&
                 nextPlanned == null)
        {
          arriving = PlannerUtils.copyTime(arriving, timestampTimeModel, arriving, eventEffectiveFromTimeModel);
          arriving = eventEffectiveToTimeModel.setFarFutureTime(arriving);
          arriving = systemEffectiveFromTimeModel.setCurrentSystemTime(arriving);
          arriving = systemEffectiveToTimeModel.setFarFutureTime(arriving);
          if (hasCurrentFlagField()) {
            arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), getCurrentFlagYesValue());
          }
          arriving = PlannerUtils.setMutationType(arriving, MutationType.INSERT);
          plannedForKey.add(arriving);
          
          if (hasCurrentFlagField()) {
            plan = RowUtils.set(plan, getCurrentFlagFieldName(), getCurrentFlagNoValue());
          }
          plannedForKey.set(position, plan);

          // We only need to supersede a record that has already become visible in the output.
          // We know this has happened if the record in the plan has an earlier system
          // time than the current system time.
          if (PlannerUtils.before(systemEffectiveFromTimeModel, plan, currentSystemTimeRow)) {
            plan = systemEffectiveToTimeModel.setPrecedingSystemTime(plan);
            if (!PlannerUtils.getMutationType(plan).equals(MutationType.INSERT)) {
              plan = PlannerUtils.setMutationType(plan, MutationType.UPDATE);
            }
            plannedForKey.set(position, plan);
            
            plan = PlannerUtils.copyPrecedingTime(arriving, timestampTimeModel, plan, eventEffectiveToTimeModel);
            plan = systemEffectiveFromTimeModel.setCurrentSystemTime(plan);
            plan = systemEffectiveToTimeModel.setFarFutureTime(plan);
            if (hasCurrentFlagField()) {
              plan = RowUtils.set(plan, getCurrentFlagFieldName(), getCurrentFlagNoValue());
            }
            plan = PlannerUtils.setMutationType(plan, MutationType.INSERT);
            plannedForKey.add(plan);
          }
          else {
            plan = PlannerUtils.copyPrecedingTime(arriving, timestampTimeModel, plan, eventEffectiveToTimeModel);
            plannedForKey.set(position, plan);
          }
          
          break;
        }
      }

      Collections.sort(plannedForKey, timestampTimeModel);
    }

    // Final pass-through here to carry forward anything we need to
    List<Row> planned = Lists.newArrayList();
    if (doesCarryForward()) {
      for (int position = 0; position < plannedForKey.size(); position++) {
        Row plan = plannedForKey.get(position);
        // We carry forward for all mutations in case the next non-NONE row needs the values from this row
        if (position > 0) {
          Row carried = carryForwardWhenNull(plan, plannedForKey.get(position - 1));
          if (RowUtils.different(plan, carried, getValueFieldNames())) {
            // Close existing record and add a new one if not an insert - otherwise just replace
            plan = systemEffectiveToTimeModel.setPrecedingSystemTime(plan);
            if (hasCurrentFlagField()) {
              plan = RowUtils.set(plan, getCurrentFlagFieldName(), getCurrentFlagNoValue());
            }
            if (PlannerUtils.getMutationType(plan).equals(MutationType.INSERT)) {
              plannedForKey.set(position, carried);
              plan = carried;
              // This might supersede a previous insert that we've just added as part of a history re-write
              // Condition: the values are the same, it has the same timestamp and has the same currentSystemTime
              if (planned.size() > 1 &&
                  PlannerUtils.simultaneous(timestampTimeModel, plan, planned.get(planned.size() - 1)) &&
                  PlannerUtils.simultaneous(systemEffectiveFromTimeModel, plan, planned.get(planned.size() - 1)))
              {
                planned.remove(planned.size() - 1);
              }
              planned.add(plan);
            } else {
              planned.add(PlannerUtils.setMutationType(plan, MutationType.UPDATE));
              carried = systemEffectiveFromTimeModel.setCurrentSystemTime(carried);
              carried = systemEffectiveToTimeModel.setFarFutureTime(carried);
              if (hasCurrentFlagField()) {
                carried = RowUtils.set(carried, getCurrentFlagFieldName(), getCurrentFlagYesValue());
              }
              planned.add(PlannerUtils.setMutationType(carried, MutationType.INSERT));
            }
          } else if (!PlannerUtils.getMutationType(plan).equals(MutationType.NONE)) {
            planned.add(plan);
          }
          plannedForKey.set(position, carried);
        } else if (!PlannerUtils.getMutationType(plan).equals(MutationType.NONE)) {
          planned.add(plan);
        }
      }
    } else {
      for (Row plan : plannedForKey) {
        if (!PlannerUtils.getMutationType(plan).equals(MutationType.NONE)) {
          planned.add(plan);
        }
      }
    }

    return planned;
  }

  @Override
  public List<String> getKeyFieldNames() {
    return config.getStringList(KEY_FIELD_NAMES_CONFIG_NAME);
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

  private String getCurrentFlagFieldName() {
    return config.getString(CURRENT_FLAG_FIELD_NAME_CONFIG_NAME);
  }

  private String getCurrentFlagYesValue(){
    return hasCurrentFlagYes() ? config.getString(CURRENT_FLAG_YES_CONFIG_NAME) : CURRENT_FLAG_DEFAULT_YES;
  }

  private String getCurrentFlagNoValue(){
    return hasCurrentFlagNo() ? config.getString(CURRENT_FLAG_NO_CONFIG_NAME) : CURRENT_FLAG_DEFAULT_NO;
  }

  private List<String> getEventTimeEffectiveToFieldNames() {
    return config.getStringList(EVENT_TIME_EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME);
  }

  private List<String> getEventTimeEffectiveFromFieldNames() {
    return config.getStringList(EVENT_TIME_EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME);
  }

  private List<String> getSystemTimeEffectiveToFieldNames() {
    return config.getStringList(SYSTEM_TIME_EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME);
  }

  private List<String> getSystemTimeEffectiveFromFieldNames() {
    return config.getStringList(SYSTEM_TIME_EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME);
  }

  private List<String> getValueFieldNames() {
    return config.getStringList(VALUE_FIELD_NAMES_CONFIG_NAME);
  }

  private List<String> getTimestampFieldNames() {
    return config.getStringList(TIMESTAMP_FIELD_NAMES_CONFIG_NAME);
  }

  private boolean doesCarryForward() {
    return ConfigUtils.getOrElse(config, CARRY_FORWARD_CONFIG_NAME, false);
  }

  // When the arrived record value is null then we have the option to carry forward
  // the value from the previous record. This is useful for handling sparse records.
  private Row carryForwardWhenNull(Row into, Row from) {
    if (!doesCarryForward()) {
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
    
    this.timestampTimeModel.configureCurrentSystemTime(currentSystemTimeMillis);
    this.eventEffectiveFromTimeModel.configureCurrentSystemTime(currentSystemTimeMillis);
    this.eventEffectiveToTimeModel.configureCurrentSystemTime(currentSystemTimeMillis);
    this.systemEffectiveFromTimeModel.configureCurrentSystemTime(currentSystemTimeMillis);
    this.systemEffectiveToTimeModel.configureCurrentSystemTime(currentSystemTimeMillis);
    this.currentSystemTimeRow = getCurrentSystemTimeRow(currentSystemTimeMillis);
  }
  
  private Row getCurrentSystemTimeRow(long currentSystemTimeMillis) {
    StructType schema = 
        RowUtils.appendFields(systemEffectiveFromTimeModel.getSchema(), 
            Lists.newArrayList(systemEffectiveToTimeModel.getSchema().fields()));
    Object[] nulls = new Object[schema.size()];
    Row row = new RowWithSchema(schema, nulls);
    row = systemEffectiveFromTimeModel.setCurrentSystemTime(row);
    row = systemEffectiveToTimeModel.setCurrentSystemTime(row);
    
    return row;
  }
  
  private Config getEventTimeModelConfig() {
    return config.hasPath(EVENT_TIME_MODEL_CONFIG_NAME) ? 
        config.getConfig(EVENT_TIME_MODEL_CONFIG_NAME) : ConfigFactory.empty();
  }
  
  private Config getSystemTimeModelConfig() {
    return config.hasPath(SYSTEM_TIME_MODEL_CONFIG_NAME) ? 
        config.getConfig(SYSTEM_TIME_MODEL_CONFIG_NAME) : ConfigFactory.empty();
  }

  private TimeModel getTimestampTimeModel(boolean configure) {
    return TimeModelFactory.create(
        getEventTimeModelConfig(), getTimestampFieldNames(), configure);
  }

  private TimeModel getEventEffectiveFromTimeModel(boolean configure) {
    return TimeModelFactory.create(
        getEventTimeModelConfig(), getEventTimeEffectiveFromFieldNames(), configure);
  }

  private TimeModel getEventEffectiveToTimeModel(boolean configure) {
    return TimeModelFactory.create(
        getEventTimeModelConfig(), getEventTimeEffectiveToFieldNames(), configure);
  }

  private TimeModel getSystemEffectiveFromTimeModel(boolean configure) {
    return TimeModelFactory.create(
        getSystemTimeModelConfig(), getSystemTimeEffectiveFromFieldNames(), configure);
  }

  private TimeModel getSystemEffectiveToTimeModel(boolean configure) {
    return TimeModelFactory.create(
        getSystemTimeModelConfig(), getSystemTimeEffectiveToFieldNames(), configure);
  }
  
  @Override
  public String getAlias() {
    return "bitemporal";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(KEY_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(VALUE_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(TIMESTAMP_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(EVENT_TIME_EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(EVENT_TIME_EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(SYSTEM_TIME_EFFECTIVE_FROM_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .mandatoryPath(SYSTEM_TIME_EFFECTIVE_TO_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST)
        .optionalPath(CURRENT_FLAG_FIELD_NAME_CONFIG_NAME, ConfigValueType.STRING)
        .optionalPath(CURRENT_FLAG_YES_CONFIG_NAME, ConfigValueType.STRING)
        .optionalPath(CURRENT_FLAG_NO_CONFIG_NAME, ConfigValueType.STRING)
        .optionalPath(CARRY_FORWARD_CONFIG_NAME, ConfigValueType.BOOLEAN)
        .optionalPath(EVENT_TIME_MODEL_CONFIG_NAME, ConfigValueType.OBJECT)
        .optionalPath(SYSTEM_TIME_MODEL_CONFIG_NAME, ConfigValueType.OBJECT)
        .handlesOwnValidationPath(EVENT_TIME_MODEL_CONFIG_NAME)
        .handlesOwnValidationPath(SYSTEM_TIME_MODEL_CONFIG_NAME)
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    this.config = config;

    Set<InstantiatedComponent> components = Sets.newHashSet();

    components.add(new InstantiatedComponent(
        getEventEffectiveFromTimeModel(configure), getEventTimeModelConfig(), "Event Time Model"));
    components.add(new InstantiatedComponent(
        getSystemEffectiveFromTimeModel(configure), getSystemTimeModelConfig(), "System Time Model"));

    return components;
  }
  
}
