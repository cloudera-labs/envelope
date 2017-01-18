package com.cloudera.labs.envelope.plan;

import java.lang.reflect.Constructor;
import java.util.Set;

import com.cloudera.labs.envelope.plan.bulk.AppendPlanner;
import com.cloudera.labs.envelope.plan.bulk.OverwritePlanner;
import com.cloudera.labs.envelope.plan.bulk.SystemTimeUpsertPlanner;
import com.cloudera.labs.envelope.plan.random.EventTimeHistoryPlanner;
import com.cloudera.labs.envelope.plan.random.EventTimeUpsertPlanner;
import com.typesafe.config.Config;

public abstract class Planner {
    
    public static final String TYPE_CONFIG_NAME = "type";
    
    protected Config config;
    
    public Planner(Config config) {
        this.config = config;
    }
    
    public abstract Set<MutationType> getEmittedMutationTypes();
    
    public static Planner plannerFor(Config plannerConfig) throws Exception {
        if (!plannerConfig.hasPath(TYPE_CONFIG_NAME)) {
            throw new RuntimeException("Planner type not specified");
        }
        
        String plannerType = plannerConfig.getString(TYPE_CONFIG_NAME);
        
        Planner planner;
        
        switch (plannerType) {
            case "append":
                planner = new AppendPlanner(plannerConfig);
                break;
            case "upsert":
                planner = new SystemTimeUpsertPlanner(plannerConfig);
                break;
            case "overwrite":
                planner = new OverwritePlanner(plannerConfig);
                break;
            case "eventtimeupsert":
                planner = new EventTimeUpsertPlanner(plannerConfig);
                break;
            case "history":
                planner = new EventTimeHistoryPlanner(plannerConfig);
                break;
            default:
                Class<?> clazz = Class.forName(plannerType);
                Constructor<?> constructor = clazz.getConstructor(Config.class);
                planner = (Planner)constructor.newInstance(plannerConfig);
        }
        
        return planner;
    }
    
}
