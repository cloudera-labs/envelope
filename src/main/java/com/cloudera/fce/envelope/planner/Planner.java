package com.cloudera.fce.envelope.planner;

import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;

import com.cloudera.fce.envelope.RecordModel;
import com.cloudera.fce.envelope.utils.PropertiesUtils;

public abstract class Planner {
    
    protected Properties props;
    
    public Planner(Properties props) throws Exception {
        this.props = props;
    }
    
    public abstract List<PlannedRecord> planOperations(List<GenericRecord> arrivingRecords,
            List<GenericRecord> existingRecords, RecordModel recordModel) throws Exception;
    
    public static String currentTimestampString() {
        return new Date(System.currentTimeMillis()).toString();
    }
    
    public abstract boolean requiresExisting();
    
    public static Planner plannerFor(Properties props) throws Exception {
        String plannerName = props.getProperty("planner");
        Properties plannerProps = PropertiesUtils.prefixProperties(props, "planner.");
        
        Planner planner = null;

        switch (plannerName) {
            case "upsert":
                planner = new UpsertPlanner(plannerProps);
                break;
            case "history":
                planner = new HistoryPlanner(plannerProps);
                break;
            case "insertonly":
                planner = new InsertOnlyPlanner(plannerProps);
                break;
            default:
                Class<?> clazz = Class.forName(plannerName);
                Constructor<?> constructor = clazz.getConstructor();
                planner = (Planner)constructor.newInstance(plannerProps);
        }
        
        return planner;
    }

}
