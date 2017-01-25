package com.cloudera.labs.envelope.plan;

import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

public class SystemTimeUpsertPlanner implements BulkPlanner {
    
    public final static String LAST_UPDATED_FIELD_NAME_CONFIG_NAME = "field.last.updated";
    
    private Config config;
    
    @Override
    public void configure(Config config) {
        this.config = config;
    }
    
    @Override
    public List<Tuple2<MutationType, DataFrame>> planMutationsForSet(DataFrame arriving)
    {
        if (hasLastUpdatedField()) {
            arriving = arriving.withColumn(getLastUpdatedFieldName(), functions.lit(currentTimestampString()));
        }
        
        List<Tuple2<MutationType, DataFrame>> planned = Lists.newArrayList();
        
        planned.add(new Tuple2<MutationType, DataFrame>(MutationType.UPSERT, arriving));
        
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
