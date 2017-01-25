package com.cloudera.labs.envelope.plan;

import java.lang.reflect.Constructor;

import com.typesafe.config.Config;

public class PlannerFactory {
    
    public static final String TYPE_CONFIG_NAME = "type";

    public static Planner create(Config plannerConfig) throws Exception {
        if (!plannerConfig.hasPath(TYPE_CONFIG_NAME)) {
            throw new RuntimeException("Planner type not specified");
        }
        
        String plannerType = plannerConfig.getString(TYPE_CONFIG_NAME);
        
        Planner planner;
        
        switch (plannerType) {
            case "append":
                planner = new AppendPlanner();
                break;
            case "upsert":
                planner = new SystemTimeUpsertPlanner();
                break;
            case "overwrite":
                planner = new OverwritePlanner();
                break;
            case "eventtimeupsert":
                planner = new EventTimeUpsertPlanner();
                break;
            case "history":
                planner = new EventTimeHistoryPlanner();
                break;
            case "bitemporal":
                planner = new BitemporalHistoryPlanner();
                break;
            default:
                Class<?> clazz = Class.forName(plannerType);
                Constructor<?> constructor = clazz.getConstructor();
                planner = (Planner)constructor.newInstance();
        }
        
        planner.configure(plannerConfig);
        
        return planner;
    }
    
}
