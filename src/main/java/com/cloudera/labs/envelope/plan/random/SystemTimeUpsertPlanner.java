package com.cloudera.labs.envelope.plan.random;

import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

public class SystemTimeUpsertPlanner extends RandomWritePlanner {
    
    public final static String LAST_UPDATED_FIELD_NAME_CONFIG_NAME = "field.last.updated";
    
    public SystemTimeUpsertPlanner(Config config) {
        super(config);
    }
    
    @Override
    public List<PlannedRow> planMutationsForRow(Row arriving)
    {
        if (arriving.schema() == null) {
            throw new RuntimeException("Arriving row sent to system time upsert planner does not contain a schema");
        }
        
        if (hasLastUpdatedField()) {
            arriving = RowUtils.append(arriving, getLastUpdatedFieldName(), DataTypes.StringType, currentTimestampString());
        }
        
        PlannedRow plan = new PlannedRow(arriving, MutationType.UPSERT);
        List<PlannedRow> planned = Lists.newArrayList(plan);
        
        return planned;
    }
    
    @Override
    public Set<MutationType> getEmittedMutationTypes() {
        return Sets.newHashSet(MutationType.UPSERT);
    }
    
    private String currentTimestampString() {
        return new Date(System.currentTimeMillis()).toString();
    }
    
    private boolean hasLastUpdatedField() {
        return config.hasPath(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
    }
    
    private String getLastUpdatedFieldName() {
        return config.getString(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
    }
    
}
