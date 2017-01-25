package com.cloudera.labs.envelope.plan;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

/**
 * A planner implementation for storing all versions of the values of a key (its history) using
 * Type II SCD modeling.
 */
public class EventTimeHistoryPlanner implements RandomPlanner {
    
    public static final String KEY_FIELD_NAMES_CONFIG_NAME = "fields.key";
    public static final String VALUE_FIELD_NAMES_CONFIG_NAME = "fields.values";
    public static final String TIMESTAMP_FIELD_NAME_CONFIG_NAME = "field.timestamp";
    public static final String EFFECTIVE_FROM_FIELD_NAME_CONFIG_NAME = "field.effective.from";
    public static final String EFFECTIVE_TO_FIELD_NAME_CONFIG_NAME = "field.effective.to";
    public static final String CURRENT_FLAG_FIELD_NAME_CONFIG_NAME = "field.current.flag";
    public static final String LAST_UPDATED_FIELD_NAME_CONFIG_NAME = "field.last.updated";
    public static final String CARRY_FORWARD_CONFIG_NAME = "carry.forward.when.null";
    
    public static final String CURRENT_FLAG_YES = "Y";
    public static final String CURRENT_FLAG_NO = "N";
    public static final Long FAR_FUTURE_MILLIS = 253402214400000L; // 9999-12-31
    
    private Config config;
    
    @Override
    public void configure(Config config) {
        this.config = config;
    }
    
    @Override
    public List<PlannedRow> planMutationsForKey(Row key, List<Row> arrivingForKey, List<Row> existingForKey)
    {   
        Comparator<PlannedRow> tc = new PlanTimestampComparator(getTimestampFieldName());
        
        List<PlannedRow> planned = Lists.newArrayList();
        List<PlannedRow> plannedForKey = Lists.newArrayList();
        
        if (existingForKey != null) {
            for (Row existing : existingForKey) {
                plannedForKey.add(new PlannedRow(existing, MutationType.NONE));
            }
        }
        
        Collections.sort(plannedForKey, tc);
        
        for (Row arriving : arrivingForKey) {
            arriving = RowUtils.append(arriving, getEffectiveFromFieldName(), DataTypes.LongType, null);
            arriving = RowUtils.append(arriving, getEffectiveToFieldName(), DataTypes.LongType, null);
            if (hasCurrentFlagField()) {
                arriving = RowUtils.append(arriving, getCurrentFlagFieldName(), DataTypes.StringType, null);
            }
            if (hasLastUpdatedField()) {
                arriving = RowUtils.append(arriving, getLastUpdatedFieldName(), DataTypes.StringType, null);
            }
            
            Long arrivedTimestamp = (Long)RowUtils.get(arriving, getTimestampFieldName());
            
            // There was no existing record for the key, so we just insert the input record.
            if (plannedForKey.size() == 0) {
                arriving = RowUtils.set(arriving, getEffectiveFromFieldName(), arrivedTimestamp);
                arriving = RowUtils.set(arriving, getEffectiveToFieldName(), FAR_FUTURE_MILLIS);
                if (hasCurrentFlagField()) {
                    arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), CURRENT_FLAG_YES);
                }
                if (hasLastUpdatedField()) {
                    arriving = RowUtils.set(arriving, getLastUpdatedFieldName(), currentTimestampString());
                }
                plannedForKey.add(new PlannedRow(arriving, MutationType.INSERT));
                
                continue;
            }
            
            // Iterate through each existing record of the key in time order, stopping when we
            // have either corrected the history or gone all the way through it.
            for (int position = 0; position < plannedForKey.size(); position++) {
                PlannedRow plan = plannedForKey.get(position);
                Long planTimestamp = (Long)RowUtils.get(plan.getRow(), getTimestampFieldName());
                PlannedRow previousPlanned = null;
                PlannedRow nextPlanned = null;
                Long nextPlannedTimestamp = null;
                
                if (position > 0) {
                    previousPlanned = plannedForKey.get(position - 1);
                }
                if (position + 1 < plannedForKey.size()) {
                    nextPlanned = plannedForKey.get(position + 1);
                    nextPlannedTimestamp = (Long)RowUtils.get(nextPlanned.getRow(), getTimestampFieldName());
                }
                
                // There is an existing record for the same key and timestamp. It is possible that
                // the existing record is in the storage layer or is about to be added during this
                // micro-batch. Either way, we only update that record if it has changed.
                if (RowUtils.simultaneous(arriving, plan.getRow(), getTimestampFieldName()) &&
                    RowUtils.different(arriving, plan.getRow(), getValueFieldNames()))
                {
                    arriving = RowUtils.set(arriving, getEffectiveFromFieldName(), RowUtils.get(plan.getRow(), getEffectiveFromFieldName()));
                    arriving = RowUtils.set(arriving, getEffectiveToFieldName(), RowUtils.get(plan.getRow(), getEffectiveToFieldName()));
                    if (hasCurrentFlagField()) {
                        arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), RowUtils.get(plan.getRow(), getCurrentFlagFieldName()));
                    }
                    if (hasLastUpdatedField()) {
                        arriving = RowUtils.set(arriving, getLastUpdatedFieldName(), currentTimestampString());
                    }
                    
                    if (plan.getMutationType().equals(MutationType.INSERT)) {
                        plannedForKey.set(position, new PlannedRow(arriving, MutationType.INSERT));
                    }
                    else {
                        plannedForKey.set(position, new PlannedRow(arriving, MutationType.UPDATE));
                    }
                    
                    break;
                }
                // Before them all
                // -> Insert with ED just before first
                // The input record is timestamped before any existing record of the same key. In
                // this case there is no need to modify existing records, and we only have to insert
                // the input record as effective up until just prior to the first existing record.
                else if (previousPlanned == null && RowUtils.before(arriving, plan.getRow(), getTimestampFieldName())) {
                    arriving = RowUtils.set(arriving, getEffectiveFromFieldName(), arrivedTimestamp);
                    arriving = RowUtils.set(arriving, getEffectiveToFieldName(), RowUtils.precedingTimestamp(planTimestamp));
                    if (hasCurrentFlagField()) {
                        arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), CURRENT_FLAG_NO);
                    }
                    if (hasLastUpdatedField()) {
                        arriving = RowUtils.set(arriving, getLastUpdatedFieldName(), currentTimestampString());
                    }
                    plannedForKey.add(new PlannedRow(arriving, MutationType.INSERT));
                    
                    break;
                }
                // The input record is timestamped with an existing record of the same key before it
                // and an existing record of the same key after it. We insert the input record
                // effective until just prior to the next existing record and we update the
                // previous existing record to be effective until just prior to the input record.
                else if (plan != null && nextPlanned != null &&
                         RowUtils.after(arriving, plan.getRow(), getTimestampFieldName()) &&
                         RowUtils.before(arriving, nextPlanned.getRow(), getTimestampFieldName()))
                {
                    arriving = RowUtils.set(arriving, getEffectiveFromFieldName(), arrivedTimestamp);
                    arriving = RowUtils.set(arriving, getEffectiveToFieldName(), RowUtils.precedingTimestamp(nextPlannedTimestamp));
                    if (hasCurrentFlagField()) {
                        arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), CURRENT_FLAG_NO);
                    }
                    if (hasLastUpdatedField()) {
                        arriving = RowUtils.set(arriving, getLastUpdatedFieldName(), currentTimestampString());
                    }
                    carryForwardWhenNull(arriving, plan.getRow());
                    plannedForKey.add(new PlannedRow(arriving, MutationType.INSERT));
                    
                    plan.setRow(RowUtils.set(plan.getRow(), getEffectiveToFieldName(), RowUtils.precedingTimestamp(arrivedTimestamp)));
                    if (hasCurrentFlagField()) {
                        plan.setRow(RowUtils.set(plan.getRow(), getCurrentFlagFieldName(), CURRENT_FLAG_NO));
                    }
                    if (hasLastUpdatedField()) {
                        plan.setRow(RowUtils.set(plan.getRow(), getLastUpdatedFieldName(), currentTimestampString()));
                    }
                    if (!plan.getMutationType().equals(MutationType.INSERT)) {
                        plan.setMutationType(MutationType.UPDATE);
                    }
                    
                    break;
                }
                // The input record is arriving after all existing records of the same key. This
                // is the 'normal' case where data arrives in order. We insert the input record
                // effective until the far future, and we update the previous existing record
                // to be effective until just prior to the input record.
                else if (RowUtils.after(arriving, plan.getRow(), getTimestampFieldName()) && nextPlanned == null) {
                    arriving = RowUtils.set(arriving, getEffectiveFromFieldName(), arrivedTimestamp);
                    arriving = RowUtils.set(arriving, getEffectiveToFieldName(), FAR_FUTURE_MILLIS);
                    if (hasCurrentFlagField()) {
                        arriving = RowUtils.set(arriving, getCurrentFlagFieldName(), CURRENT_FLAG_YES);
                    }
                    if (hasLastUpdatedField()) {
                        arriving = RowUtils.set(arriving, getLastUpdatedFieldName(), currentTimestampString());
                    }
                    carryForwardWhenNull(arriving, plan.getRow());
                    plannedForKey.add(new PlannedRow(arriving, MutationType.INSERT));
                    
                    plan.setRow(RowUtils.set(plan.getRow(), getEffectiveToFieldName(), RowUtils.precedingTimestamp(arrivedTimestamp)));
                    if (hasCurrentFlagField()) {
                        plan.setRow(RowUtils.set(plan.getRow(), getCurrentFlagFieldName(), CURRENT_FLAG_NO));
                    }
                    if (hasLastUpdatedField()) {
                        plan.setRow(RowUtils.set(plan.getRow(), getLastUpdatedFieldName(), currentTimestampString()));
                    }
                    if (!plan.getMutationType().equals(MutationType.INSERT)) {
                        plan.setMutationType(MutationType.UPDATE);
                    }
                    
                    break;
                }
            }
            
            Collections.sort(plannedForKey, tc);
        }
        
        for (PlannedRow plan : plannedForKey) {
            if (!plan.getMutationType().equals(MutationType.NONE)) {
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

    private String getLastUpdatedFieldName() {
        return config.getString(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
    }

    private String getCurrentFlagFieldName() {
        return config.getString(CURRENT_FLAG_FIELD_NAME_CONFIG_NAME);
    }

    private String getEffectiveToFieldName() {
        return config.getString(EFFECTIVE_TO_FIELD_NAME_CONFIG_NAME);
    }

    private String getEffectiveFromFieldName() {
        return config.getString(EFFECTIVE_FROM_FIELD_NAME_CONFIG_NAME);
    }

    private List<String> getValueFieldNames() {
        return config.getStringList(VALUE_FIELD_NAMES_CONFIG_NAME);
    }

    private String getTimestampFieldName() {
        return config.getString(TIMESTAMP_FIELD_NAME_CONFIG_NAME);
    }

    // When the arrived record value is null then we have the option to carry forward
    // the value from the previous record. This is useful for handling sparse stream records.
    private void carryForwardWhenNull(Row into, Row from) {
        if (!config.hasPath(CARRY_FORWARD_CONFIG_NAME) || !config.getBoolean(CARRY_FORWARD_CONFIG_NAME)) {
            return;
        }
        
        for (StructField field : into.schema().fields()) {
            String fieldName = field.name();
            if (RowUtils.get(into, fieldName) == null && RowUtils.get(from, fieldName) != null) {
                into = RowUtils.set(into, fieldName, RowUtils.get(from, fieldName));
            }
        }
    }
    
    @Override
    public Set<MutationType> getEmittedMutationTypes() {
        return Sets.newHashSet(MutationType.INSERT, MutationType.UPDATE);
    }
    
    private String currentTimestampString() {
        return new Date(System.currentTimeMillis()).toString();
    }
    
    private class PlanTimestampComparator implements Comparator<PlannedRow> {
        private String timestampFieldName;
        
        public PlanTimestampComparator(String timestampFieldName) {
            this.timestampFieldName = timestampFieldName;
        }
        
        @Override
        public int compare(PlannedRow p1, PlannedRow p2) {
            return RowUtils.compareTimestamp(p1.getRow(), p2.getRow(), timestampFieldName);
        }
    }
    
}
