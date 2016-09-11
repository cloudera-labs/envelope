package com.cloudera.labs.envelope.plan.random;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

/**
 * A planner implementation for appending the stream to the storage table. Only plans insert mutations.
 */
public class AppendPlanner extends RandomWritePlanner {
    
    public final static String KEY_FIELD_NAMES_CONFIG_NAME = "fields.key";
    public final static String LAST_UPDATED_FIELD_NAME_CONFIG_NAME = "field.last.updated";
    public final static String UUID_KEY_CONFIG_NAME = "uuid.key.enabled";
    
    public AppendPlanner(Config config) {
        super(config);
    }
    
    @Override
    public List<PlannedRow> planMutationsForRow(Row arriving)
    {
        if (arriving.schema() == null) {
            throw new RuntimeException("Arriving row sent to append planner does not contain a schema");
        } 
        
        if (setsKeyToUUID()) {
            if (!hasKeyFields()) {
                throw new RuntimeException("Key columns must be specified to provide UUID keys");
            }
            
            arriving = RowUtils.set(arriving, getKeyFieldNames().get(0), UUID.randomUUID().toString());
        }
        
        if (hasLastUpdatedField()) {
            arriving = RowUtils.append(arriving, getLastUpdatedFieldName(), DataTypes.StringType, currentTimestampString());
        }
        
        PlannedRow plan = new PlannedRow(arriving, MutationType.INSERT);
        List<PlannedRow> planned = Lists.newArrayList(plan);
        
        return planned;
    }

    @Override
    public Set<MutationType> getEmittedMutationTypes() {
        return Sets.newHashSet(MutationType.INSERT);
    }
    
    private String currentTimestampString() {
        return new Date(System.currentTimeMillis()).toString();
    }
    
    private boolean hasKeyFields() {
        return config.hasPath(KEY_FIELD_NAMES_CONFIG_NAME);
    }
    
    private List<String> getKeyFieldNames() {
        return config.getStringList(KEY_FIELD_NAMES_CONFIG_NAME);
    }
    
    private boolean hasLastUpdatedField() {
        return config.hasPath(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
    }
    
    private String getLastUpdatedFieldName() {
        return config.getString(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
    }
    
    private boolean setsKeyToUUID() {
        if (!config.hasPath(UUID_KEY_CONFIG_NAME)) return false;
        
        return Boolean.parseBoolean(config.getString(UUID_KEY_CONFIG_NAME));
    }
    
}
