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
package com.cloudera.labs.envelope.plan;

import static com.cloudera.labs.envelope.utils.ConfigUtils.assertConfig;
import static com.cloudera.labs.envelope.utils.RowUtils.*;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

public class BitemporalHistoryPlanner implements RandomPlanner {

  public static final String KEY_FIELD_NAMES_CONFIG_NAME = "fields.key";
  public static final String VALUE_FIELD_NAMES_CONFIG_NAME = "fields.values";
  public static final String TIMESTAMP_FIELD_NAME_CONFIG_NAME = "field.timestamp";
  public static final String EVENT_TIME_EFFECTIVE_FROM_FIELD_NAME_CONFIG_NAME = "field.event.time.effective.from";
  public static final String EVENT_TIME_EFFECTIVE_TO_FIELD_NAME_CONFIG_NAME = "field.event.time.effective.to";
  public static final String SYSTEM_TIME_EFFECTIVE_FROM_FIELD_NAME_CONFIG_NAME = "field.system.time.effective.from";
  public static final String SYSTEM_TIME_EFFECTIVE_TO_FIELD_NAME_CONFIG_NAME = "field.system.time.effective.to";
  public static final String CURRENT_FLAG_FIELD_NAME_CONFIG_NAME = "field.current.flag";
  public static final String CARRY_FORWARD_CONFIG_NAME = "carry.forward.when.null";

  public static final String CURRENT_FLAG_YES = "Y";
  public static final String CURRENT_FLAG_NO = "N";
  public static final Long FAR_FUTURE_MILLIS = 253402214400000L; // 9999-12-31

  private Config config;

  @Override
  public void configure(Config config) {
    this.config = config;
    assertConfig(config, KEY_FIELD_NAMES_CONFIG_NAME);
    assertConfig(config, VALUE_FIELD_NAMES_CONFIG_NAME);
    assertConfig(config, TIMESTAMP_FIELD_NAME_CONFIG_NAME);
    assertConfig(config, EVENT_TIME_EFFECTIVE_FROM_FIELD_NAME_CONFIG_NAME);
    assertConfig(config, EVENT_TIME_EFFECTIVE_TO_FIELD_NAME_CONFIG_NAME);
    assertConfig(config, SYSTEM_TIME_EFFECTIVE_FROM_FIELD_NAME_CONFIG_NAME);
    assertConfig(config, SYSTEM_TIME_EFFECTIVE_TO_FIELD_NAME_CONFIG_NAME);
  }

  @Override
  public Set<MutationType> getEmittedMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT, MutationType.UPDATE);
  }

  @Override
  public List<PlannedRow> planMutationsForKey(Row key, List<Row> arrivingForKey, List<Row> existingForKey) {
    List<String> valueFieldNames = getValueFieldNames();
    String timestampFieldName = getTimestampFieldName();
    String eventTimeEffectiveFromFieldName = getEventTimeEffectiveFromFieldName();
    String eventTimeEffectiveToFieldName = getEventTimeEffectiveToFieldName();
    String systemTimeEffectiveFromFieldName = getSystemTimeEffectiveFromFieldName();
    String systemTimeEffectiveToFieldName = getSystemTimeEffectiveToFieldName();
    boolean hasCurrentFlagField = hasCurrentFlagField();
    String currentFlagFieldName = null;
    if (hasCurrentFlagField) {
      currentFlagFieldName = getCurrentFlagFieldName();
    }

    long currentSystemTime = System.currentTimeMillis();
    Comparator<PlannedRow> tc = new PlanTimestampComparator(timestampFieldName);

    List<PlannedRow> plannedForKey = Lists.newArrayList();

    // Filter out existing entries for this key that have already been closed
    if (existingForKey != null) {
      for (Row existing : existingForKey) {
        if (currentSystemTime < (long)get(existing, systemTimeEffectiveToFieldName)) {
          plannedForKey.add(new PlannedRow(existing, MutationType.NONE));
        }
      }
    }

    Collections.sort(plannedForKey, tc);
    Collections.sort(arrivingForKey, new ArrivingTimestampComparator(timestampFieldName));

    for (Row arriving : arrivingForKey) {
      arriving = append(arriving, eventTimeEffectiveFromFieldName, DataTypes.LongType, null);
      arriving = append(arriving, eventTimeEffectiveToFieldName, DataTypes.LongType, null);
      arriving = append(arriving, systemTimeEffectiveFromFieldName, DataTypes.LongType, null);
      arriving = append(arriving, systemTimeEffectiveToFieldName, DataTypes.LongType, null);
      if (hasCurrentFlagField) {
        arriving = append(arriving, currentFlagFieldName, DataTypes.StringType, null);
      }

      Long arrivingTimestamp = (Long)get(arriving, timestampFieldName);

      // There was no existing record for the key, so we just insert the input record.
      if (plannedForKey.isEmpty()) {
        arriving = set(arriving, eventTimeEffectiveFromFieldName, arrivingTimestamp);
        arriving = set(arriving, eventTimeEffectiveToFieldName, FAR_FUTURE_MILLIS);
        arriving = set(arriving, systemTimeEffectiveFromFieldName, currentSystemTime);
        arriving = set(arriving, systemTimeEffectiveToFieldName, FAR_FUTURE_MILLIS);
        if (hasCurrentFlagField) {
          arriving = set(arriving, currentFlagFieldName, CURRENT_FLAG_YES);
        }
        plannedForKey.add(new PlannedRow(arriving, MutationType.INSERT));

        continue;
      }

      // Iterate through each existing record of the key in time order, stopping when we
      // have either corrected the history or gone all the way through it.
      for (int position = 0; position < plannedForKey.size(); position++) {
        PlannedRow plan = plannedForKey.get(position);
        Long planTimestamp = (Long)get(plan.getRow(), timestampFieldName);
        PlannedRow previousPlanned = null;
        PlannedRow nextPlanned = null;
        Long nextPlannedTimestamp = null;

        if (position > 0) {
          previousPlanned = plannedForKey.get(position - 1);
        }
        if (position + 1 < plannedForKey.size()) {
          nextPlanned = plannedForKey.get(position + 1);
          nextPlannedTimestamp = (Long)get(nextPlanned.getRow(), timestampFieldName);
        }

        // There is an existing record for the same key and timestamp. It is possible that
        // the existing record is in the storage layer or is about to be added during this
        // micro-batch. Either way, we only update that record if it has changed.
        if (simultaneous(arriving, plan.getRow(), timestampFieldName) &&
          different(arriving, plan.getRow(), valueFieldNames))
        {
          arriving = set(arriving, eventTimeEffectiveFromFieldName, get(plan.getRow(), eventTimeEffectiveFromFieldName));
          arriving = set(arriving, eventTimeEffectiveToFieldName, get(plan.getRow(), eventTimeEffectiveToFieldName));
          arriving = set(arriving, systemTimeEffectiveFromFieldName, currentSystemTime);
          arriving = set(arriving, systemTimeEffectiveToFieldName, FAR_FUTURE_MILLIS);
          if (hasCurrentFlagField) {
            arriving = set(arriving, currentFlagFieldName, get(plan.getRow(), currentFlagFieldName));
          }
          plannedForKey.add(new PlannedRow(arriving, MutationType.INSERT));

          plan.setRow(set(plan.getRow(), systemTimeEffectiveToFieldName, precedingTimestamp(currentSystemTime)));
          if (hasCurrentFlagField) {
            plan.setRow(set(plan.getRow(), currentFlagFieldName, CURRENT_FLAG_NO));
          }
          if (!plan.getMutationType().equals(MutationType.INSERT)) {
            plan.setMutationType(MutationType.UPDATE);
          }

          break;
        }
        // Before them all
        // -> Insert with ED just before first
        // The input record is timestamped before any existing record of the same key. In
        // this case there is no need to modify existing records, and we only have to insert
        // the input record as effective up until just prior to the first existing record.
        else if (previousPlanned == null && before(arriving, plan.getRow(), timestampFieldName)) {
          arriving = set(arriving, eventTimeEffectiveFromFieldName, arrivingTimestamp);
          arriving = set(arriving, eventTimeEffectiveToFieldName, precedingTimestamp(planTimestamp));
          arriving = set(arriving, systemTimeEffectiveFromFieldName, currentSystemTime);
          arriving = set(arriving, systemTimeEffectiveToFieldName, FAR_FUTURE_MILLIS);
          if (hasCurrentFlagField) {
            arriving = set(arriving, currentFlagFieldName, CURRENT_FLAG_NO);
          }
          plannedForKey.add(new PlannedRow(arriving, MutationType.INSERT));

          break;
        }
        // The input record is timestamped with an existing record of the same key before it
        // and an existing record of the same key after it. We insert the input record
        // effective until just prior to the next existing record and we update the
        // previous existing record to be effective until just prior to the input record.
        else if (plan != null && nextPlanned != null &&
             after(arriving, plan.getRow(), timestampFieldName) &&
             before(arriving, nextPlanned.getRow(), timestampFieldName))
        {
          arriving = set(arriving, eventTimeEffectiveFromFieldName, arrivingTimestamp);
          arriving = set(arriving, eventTimeEffectiveToFieldName, precedingTimestamp(nextPlannedTimestamp));
          arriving = set(arriving, systemTimeEffectiveFromFieldName, currentSystemTime);
          arriving = set(arriving, systemTimeEffectiveToFieldName, FAR_FUTURE_MILLIS);
          if (hasCurrentFlagField) {
            arriving = set(arriving, currentFlagFieldName, CURRENT_FLAG_NO);
          }
          plannedForKey.add(new PlannedRow(arriving, MutationType.INSERT));

          plan.setRow(set(plan.getRow(), systemTimeEffectiveToFieldName, precedingTimestamp(currentSystemTime)));
          if (hasCurrentFlagField) {
            plan.setRow(set(plan.getRow(), currentFlagFieldName, CURRENT_FLAG_NO));
          }
          if (!plan.getMutationType().equals(MutationType.INSERT)) {
            plan.setMutationType(MutationType.UPDATE);
          }

          Row superseded = plan.getRow();
          superseded = set(superseded, eventTimeEffectiveToFieldName, precedingTimestamp(arrivingTimestamp));
          superseded = set(superseded, systemTimeEffectiveFromFieldName, currentSystemTime);
          superseded = set(superseded, systemTimeEffectiveToFieldName, FAR_FUTURE_MILLIS);
          plannedForKey.add(new PlannedRow(superseded, MutationType.INSERT));

          break;
        }
        // The input record is arriving after all existing records of the same key. This
        // is the 'normal' case where data arrives in order. We insert the input record
        // effective until the far future, and we update the previous existing record
        // to be effective until just prior to the input record.
        else if (after(arriving, plan.getRow(), timestampFieldName) && nextPlanned == null) {
          arriving = set(arriving, eventTimeEffectiveFromFieldName, arrivingTimestamp);
          arriving = set(arriving, eventTimeEffectiveToFieldName, FAR_FUTURE_MILLIS);
          arriving = set(arriving, systemTimeEffectiveFromFieldName, currentSystemTime);
          arriving = set(arriving, systemTimeEffectiveToFieldName, FAR_FUTURE_MILLIS);
          if (hasCurrentFlagField) {
            arriving = set(arriving, currentFlagFieldName, CURRENT_FLAG_YES);
          }
          plannedForKey.add(new PlannedRow(arriving, MutationType.INSERT));

          if (hasCurrentFlagField) {
            plan.setRow(set(plan.getRow(), currentFlagFieldName, CURRENT_FLAG_NO));
          }

          if ((long)get(plan.getRow(), systemTimeEffectiveFromFieldName) < currentSystemTime) {
            plan.setRow(set(plan.getRow(), systemTimeEffectiveToFieldName, precedingTimestamp(currentSystemTime)));
            if (!plan.getMutationType().equals(MutationType.INSERT)) {
              plan.setMutationType(MutationType.UPDATE);
            }

            Row superseded = plan.getRow();
            superseded = set(superseded, eventTimeEffectiveToFieldName, precedingTimestamp(arrivingTimestamp));
            superseded = set(superseded, systemTimeEffectiveFromFieldName, currentSystemTime);
            superseded = set(superseded, systemTimeEffectiveToFieldName, FAR_FUTURE_MILLIS);
            if (hasCurrentFlagField) {
              superseded = set(superseded, currentFlagFieldName, CURRENT_FLAG_NO);
            }
            plannedForKey.add(new PlannedRow(superseded, MutationType.INSERT));
          }
          else {
            plan.setRow(set(plan.getRow(), eventTimeEffectiveToFieldName, precedingTimestamp(arrivingTimestamp)));
          }

          break;
        }
      }

      Collections.sort(plannedForKey, tc);
    }

    // Final pass-through here to carry forward anything we need to
    List<PlannedRow> planned = Lists.newArrayList();
    if (doesCarryForward()) {
      for (int position = 0; position < plannedForKey.size(); position++) {
        PlannedRow plan = plannedForKey.get(position);
        // We carry forward for all mutations in case the next non-NONE row needs the values from this row
        if (position > 0) {
          Row carried = carryForwardWhenNull(plan.getRow(),
              plannedForKey.get(position - 1).getRow());
          if (different(plan.getRow(), carried, getValueFieldNames())) {
            // Close existing record and add a new one if not an insert - otherwise just replace
            Row superseded = plan.getRow();
            superseded = set(superseded, systemTimeEffectiveToFieldName,
                precedingTimestamp(currentSystemTime));
            if (hasCurrentFlagField) {
              superseded = set(superseded, currentFlagFieldName, CURRENT_FLAG_NO);
            }
            if (plan.getMutationType().equals(MutationType.INSERT)) {
              plan.setRow(carried);
              // This might supersede a previous insert that we've just added as part of a history re-write
              // Condition: the values are the same, it has the same timestamp and has the same currentSystemTime
              if (planned.size() > 1 &&
                  simultaneous(plan.getRow(), planned.get(planned.size() - 1).getRow(),
                      timestampFieldName) &&
                  simultaneous(plan.getRow(), planned.get(planned.size() - 1).getRow(),
                      systemTimeEffectiveFromFieldName)) {
                planned.remove(planned.size() - 1);
              }
              planned.add(plan);
            } else {
              planned.add(new PlannedRow(superseded, MutationType.UPDATE));
              carried = set(carried, systemTimeEffectiveFromFieldName, currentSystemTime);
              carried = set(carried, systemTimeEffectiveToFieldName, FAR_FUTURE_MILLIS);
              if (hasCurrentFlagField) {
                carried = set(carried, currentFlagFieldName, CURRENT_FLAG_YES);
              }
              planned.add(new PlannedRow(carried, MutationType.INSERT));
            }
          } else if (!plan.getMutationType().equals(MutationType.NONE)) {
            planned.add(plan);
          }
          plan.setRow(carried);
        } else if (!plan.getMutationType().equals(MutationType.NONE)) {
          planned.add(plan);
        }
      }
    } else {
      for (PlannedRow plan : plannedForKey) {
        if (!plan.getMutationType().equals(MutationType.NONE)) {
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

  private String getCurrentFlagFieldName() {
    return config.getString(CURRENT_FLAG_FIELD_NAME_CONFIG_NAME);
  }

  private String getEventTimeEffectiveToFieldName() {
    return config.getString(EVENT_TIME_EFFECTIVE_TO_FIELD_NAME_CONFIG_NAME);
  }

  private String getEventTimeEffectiveFromFieldName() {
    return config.getString(EVENT_TIME_EFFECTIVE_FROM_FIELD_NAME_CONFIG_NAME);
  }

  private String getSystemTimeEffectiveToFieldName() {
    return config.getString(SYSTEM_TIME_EFFECTIVE_TO_FIELD_NAME_CONFIG_NAME);
  }

  private String getSystemTimeEffectiveFromFieldName() {
    return config.getString(SYSTEM_TIME_EFFECTIVE_FROM_FIELD_NAME_CONFIG_NAME);
  }

  private List<String> getValueFieldNames() {
    return config.getStringList(VALUE_FIELD_NAMES_CONFIG_NAME);
  }

  private String getTimestampFieldName() {
    return config.getString(TIMESTAMP_FIELD_NAME_CONFIG_NAME);
  }

  private boolean doesCarryForward() {
    return config.hasPath(CARRY_FORWARD_CONFIG_NAME) && config.getBoolean(CARRY_FORWARD_CONFIG_NAME);
  }

  // When the arrived record value is null then we have the option to carry forward
  // the value from the previous record. This is useful for handling sparse records.
  private Row carryForwardWhenNull(Row into, Row from) {
    if (!doesCarryForward()) {
      return into;
    }

    for (StructField field : into.schema().fields()) {
      String fieldName = field.name();
      if (get(into, fieldName) == null && get(from, fieldName) != null) {
        into = set(into, fieldName, get(from, fieldName));
      }
    }
    
    return into;
  }

  private class PlanTimestampComparator implements Comparator<PlannedRow> {
    private String timestampFieldName;

    public PlanTimestampComparator(String timestampFieldName) {
      this.timestampFieldName = timestampFieldName;
    }

    @Override
    public int compare(PlannedRow p1, PlannedRow p2) {
      return compareTimestamp(p1.getRow(), p2.getRow(), timestampFieldName);
    }
  }

  private class ArrivingTimestampComparator implements Comparator<Row> {
    private String timestampFieldName;

    public ArrivingTimestampComparator(String timestampFieldName) {
      this.timestampFieldName = timestampFieldName;
    }

    @Override
    public int compare(Row r1, Row r2) {
      return compareTimestamp(r1, r2, timestampFieldName);
    }
  }

}
