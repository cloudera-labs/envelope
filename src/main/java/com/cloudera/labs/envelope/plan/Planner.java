package com.cloudera.labs.envelope.plan;

import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;

import com.cloudera.labs.envelope.RecordModel;
import com.cloudera.labs.envelope.utils.PropertiesUtils;

/**
 * Abstract class for planners to extend.
 */
public abstract class Planner {
    
    /**
     * The properties for the planner. All planner implementations can access this object.
     */
    protected Properties props;
    
    /**
     * @param props The properties for the planner with the "*.planner." prefix removed.
     */
    public Planner(Properties props) {
        this.props = props;
    }
    
    /**
     * Plan the mutations to be made to the storage layer to effect the arrived records.
     * @param arrivingRecords The records arriving to the storage layer from the flow.
     * @param existingRecords The existing records for the keys of the arriving records.
     * @param recordModel The record model of the storage table.
     * @return The list of planned records that represent the mutations to be applied.
     */
    public abstract List<PlannedRecord> planMutations(List<GenericRecord> arrivingRecords,
            List<GenericRecord> existingRecords, RecordModel recordModel);
    
    /**
     * Plan the mutations to be made to the storage layer to effect the arrived records.
     * @param arrivingRecords The records arriving to the storage layer from the flow.
     * @param recordModel The record model of the storage table.
     * @return The list of planned records that represent the mutations to be applied.
     */
    public List<PlannedRecord> planMutations(List<GenericRecord> arrivingRecords, RecordModel recordModel) {
        return planMutations(arrivingRecords, null, recordModel);
    }
    
    /**
     * A standardized current timestamp string that all planners can optionally use for
     * last updated timestamps.
     * @return The current timestamp string.
     */
    public static String currentTimestampString() {
        return new Date(System.currentTimeMillis()).toString();
    }
    
    /**
     * Whether the planner requires the existing records of the key, in order to plan the mutations.
     * @return True if existing records are required, false otherwise.
     */
    public abstract boolean requiresExistingRecords();
    
    /**
     * Whether the planner requires the arriving records for the same key to be co-located
     * in the same RDD partition, in order to plan the mutations.
     * @return True if the records of a key must be co-located, false otherwise.
     */
    public abstract boolean requiresKeyColocation();
    
    /**
     * The mutation types that the planner may emit.
     * @return The set of mutation types.
     */
    public abstract Set<MutationType> getEmittedMutationTypes();
    
    /**
     * The planner for the flow defined in the provided properties.
     * @param props The properties of the flow.
     * @return The planner for the flow.
     */
    public static Planner plannerFor(Properties props) throws Exception {
        String plannerName = props.getProperty("planner");
        Properties plannerProps = PropertiesUtils.prefixProperties(props, "planner.");
        
        Planner planner;
        
        switch (plannerName) {
            case "append":
                planner = new AppendPlanner(plannerProps);
                break;
            case "upsert":
                planner = new UpsertPlanner(plannerProps);
                break;
            case "history":
                planner = new HistoryPlanner(plannerProps);
                break;
            default:
                Class<?> clazz = Class.forName(plannerName);
                Constructor<?> constructor = clazz.getConstructor(Properties.class);
                planner = (Planner)constructor.newInstance(plannerProps);
        }
        
        return planner;
    }
    
}
