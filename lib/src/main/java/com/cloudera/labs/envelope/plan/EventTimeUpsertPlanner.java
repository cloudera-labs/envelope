/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.plan;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.plan.time.TimeModel;
import com.cloudera.labs.envelope.plan.time.TimeModelFactory;
import com.cloudera.labs.envelope.utils.PlannerUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * A planner implementation for updating existing and inserting new (upsert). This maintains the
 * most recent version of the values of a key, which is equivalent to Type I SCD modeling.
 */
public class EventTimeUpsertPlanner implements RandomPlanner, ProvidesAlias {

  public static final String KEY_FIELD_NAMES_CONFIG_NAME = "fields.key";
  public static final String LAST_UPDATED_FIELD_NAME_CONFIG_NAME = "field.last.updated";
  public static final String TIMESTAMP_FIELD_NAMES_CONFIG_NAME = "fields.timestamp";
  public static final String VALUE_FIELD_NAMES_CONFIG_NAME = "fields.values";
  public static final String EVENT_TIME_MODEL_CONFIG_NAME = "time.model.event";
  public static final String LAST_UPDATED_TIME_MODEL_CONFIG_NAME = "time.model.last.updated";

  private Config config;
  private TimeModel eventTimeModel;
  private TimeModel lastUpdatedTimeModel;
  private List<String> valueFieldNames;

  @Override
  public void configure(Config config) {
    this.config = config;
    
    Config eventTimeModelConfig = config.hasPath(EVENT_TIME_MODEL_CONFIG_NAME) ? 
        config.getConfig(EVENT_TIME_MODEL_CONFIG_NAME) : ConfigFactory.empty();
    Config lastUpdatedTimeModelConfig = config.hasPath(LAST_UPDATED_TIME_MODEL_CONFIG_NAME) ? 
        config.getConfig(LAST_UPDATED_TIME_MODEL_CONFIG_NAME) : ConfigFactory.empty();
    
    this.eventTimeModel = TimeModelFactory.create(eventTimeModelConfig, getTimestampFieldNames());
    if (hasLastUpdatedField()) {
      this.lastUpdatedTimeModel = TimeModelFactory.create(lastUpdatedTimeModelConfig, getLastUpdatedFieldName());
    }
    this.valueFieldNames = getValueFieldNames();
  }

  @Override
  public List<Row> planMutationsForKey(Row key, List<Row> arrivingForKey, List<Row> existingForKey) {
    resetCurrentSystemTime();
    
    if (key.schema() == null) {
      throw new RuntimeException("Key sent to event time upsert planner does not contain a schema");
    }
    
    List<Row> planned = Lists.newArrayList();

    if (arrivingForKey.size() > 1) {
      Collections.sort(arrivingForKey, Collections.reverseOrder(eventTimeModel));
    }
    Row arriving = arrivingForKey.get(0);

    if (arriving.schema() == null) {
      throw new RuntimeException("Arriving row sent to event time upsert planner does not contain a schema");
    }
    
    arriving = PlannerUtils.appendMutationTypeField(arriving);
    
    if (hasLastUpdatedField()) {
      arriving = lastUpdatedTimeModel.appendFields(arriving);
    }

    Row existing = null;
    if (!existingForKey.isEmpty()) {
      existing = existingForKey.get(0);

      if (arriving.schema() == null) {
        throw new RuntimeException("Existing row sent to event time upsert planner does not contain a schema");
      }
    }

    if (existing == null) {
      if (hasLastUpdatedField()) {
        arriving = lastUpdatedTimeModel.setCurrentSystemTime(arriving);
      }

      planned.add(PlannerUtils.setMutationType(arriving, MutationType.INSERT));
    }
    else if (PlannerUtils.before(eventTimeModel, arriving, existing)) {
      // We do nothing because the arriving record is older than the existing record
    }
    else if ((PlannerUtils.simultaneous(eventTimeModel, arriving, existing) ||
              PlannerUtils.after(eventTimeModel, arriving, existing)) &&
             RowUtils.different(arriving, existing, valueFieldNames))
    {
      if (hasLastUpdatedField()) {
        arriving = lastUpdatedTimeModel.setCurrentSystemTime(arriving);
      }
      planned.add(PlannerUtils.setMutationType(arriving, MutationType.UPDATE));
    }

    return planned;
  }

  @Override
  public Set<MutationType> getEmittedMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT, MutationType.UPDATE);
  }

  @Override
  public List<String> getKeyFieldNames() {
    return config.getStringList(KEY_FIELD_NAMES_CONFIG_NAME);
  }

  private boolean hasLastUpdatedField() {
    return config.hasPath(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
  }

  private String getLastUpdatedFieldName() {
    return config.getString(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
  }

  private List<String> getValueFieldNames() {
    return config.getStringList(VALUE_FIELD_NAMES_CONFIG_NAME);
  }

  private List<String> getTimestampFieldNames() {
    return config.getStringList(TIMESTAMP_FIELD_NAMES_CONFIG_NAME);
  }
  
  private void resetCurrentSystemTime() {
    long currentSystemTimeMillis = System.currentTimeMillis();
    
    this.eventTimeModel.configureCurrentSystemTime(currentSystemTimeMillis);
    if (hasLastUpdatedField()) {
      this.lastUpdatedTimeModel.configureCurrentSystemTime(currentSystemTimeMillis);
    }
  }
  
  @Override
  public String getAlias() {
    return "eventtimeupsert";
  }

}
