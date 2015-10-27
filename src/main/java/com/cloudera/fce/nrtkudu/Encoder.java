package com.cloudera.fce.nrtkudu;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

// Base class for applying decoded Kafka messages to the storage layer.
// One encoder maps to one table in the storage layer.
// This class does not assume that the storage layer is Kudu.

public abstract class Encoder {
    
    // The current flag values used in the storage fields.
    private final String CURRENT_FLAG_YES = "Y";
    private final String CURRENT_FLAG_NO = "N";
    
    // The far future in milliseconds. We use this as a placeholder for the effective to
    // timestamp when a record version that has not been superseded. We could put NULL but that
    // makes SQL queries awkward for point-in-time analytics.
    private final Long FAR_FUTURE_MS = 253402214400000L; // 9999-12-31
    
    // Internal field placed on Avro records to indicate what operation should be done
    // on the storage layer for the record.
    protected final String OPERATION_FIELD_NAME = "__op";
    protected final String OPERATION_INSERT = "I";
    protected final String OPERATION_UPDATE = "U";
    protected final String OPERATION_NONE = "N";
    
    // The table name in the storage layer.
    protected abstract String getTableName();
    
    // The list of storage field names that constitute the natural key of the data set.
    protected abstract List<String> getKeyFieldNames();
    
    // The Avro schema that correlates to the storage table schema.
    protected abstract Schema outputSchema();
    
    // The existing storage records for the given key represented as Avro records.
    protected abstract List<GenericRecord> recordsFromOutputForKey(Map<String, Object> key) throws Exception;
    
    // The list of storage layer operations required to effect the new storage state.
    protected abstract List<Object> extractOperations(List<GenericRecord> outputs);
    
    // Apply the storage layer operations.
    protected abstract void applyOperations(List<Object> operations) throws Exception;
    
    // Connect to the storage layer.
    protected abstract void connect() throws Exception;
    
    // Disconnect from the storage layer.
    protected abstract void disconnect() throws Exception;
    
    // The list of storage field names that are considered for change data capture. This is
    // typically all fields that are not in the key and not metadata fields such as effective
    // timestamps and current flags. By default this is an empty list.
    protected List<String> getValueFieldNames() {                
        return Collections.emptyList();
    }
    
    // The storage field value derived from the input record. This allows the input record
    // to be transformed to the output storage schema. By default this maps the value of the
    // storage field name to the value of the same field name in the Avro record.
    protected Object deriveOutputField(GenericRecord input, String outputFieldName) {
        return input.get(outputFieldName);
    }
    
    // Whether the input record is applicable for this encoder. This allows the input record
    // to be filtered out if it does not meet the required criteria. By default this will consider
    // all input records for persistence in the storage layer.
    protected boolean isApplicableForOutput(GenericRecord input) {
        return true;
    }
    
    // Not yet implemented.
    // Whether the input records can contain updates to existing storage records. If this is false
    // then no storage lookups are required and the input can be immediately inserted.
    protected boolean doesContainUpdates() {
        return false;
    }
    
    // Whether the encoder tracks the history of a record over time. If this is true then the
    // storage key is assumed to be the natural key + the effective from field. If this is false
    // then the storage key is assumed to be only the natural key.
    protected boolean doesTrackHistory() {
        return false;
    }
    
    // The storage field name for the effective from timestamp of the record.
    // This can be null if the encoder does not track history.
    protected String getEffectiveFromFieldName() {
        return null;
    }
    
    // The storage field name for the effective to timestamp of the record.
    // This can be null if the encoder does not track history.
    protected String getEffectiveToFieldName() {
        return null;
    }
    
    // The storage field name for the current flag of the record.
    // This can be null if the encoder does not track history.   
    protected String getCurrentFlagFieldName() {
        return null;
    }
    
    // The storage field name for the last updated timestamp of the record.
    // This can be null if the encoder does not persist the last updated timestamp.
    protected String getLastUpdatedFieldName() {
        return null;
    }
    
    // Not yet implemented.
    // The storage field name for the hash of the values of the record. This will be used to
    // enable faster change data capture, especially on wide tables. This can be null if the
    // encoder does not use this feature.
    protected String getValueHashFieldName() {
        return null;
    };
    
    // Encode the input Avro records to the storage layer. It is guaranteed that for each
    // micro-batch that all incoming records for the same key will go to the same encode() call.
    public void encode(List<GenericRecord> inputs) throws Exception {
        // Connect to the storage layer. Connection state can be held in the extending class.
        connect();
        
        //System.out.println("inputs:");
        //System.out.println(Arrays.toString(inputs.toArray()));
        
        // Break the input record list into a list of input records per key found.
        // This allows us to iterate over the keys of the partition of the micro-batch.
        // In the process, derive the output record for the input record so that we
        // can compare it to the existing storage layer records of the key.
        Map<Map<String, Object>, List<GenericRecord>> inputsByKey = Maps.newHashMap();
        for (GenericRecord input : inputs) {
            if (isApplicableForOutput(input)) {
                Map<String, Object> key = keyFromInput(input);
                
                if (!inputsByKey.containsKey(key)) {
                    inputsByKey.put(key, new ArrayList<GenericRecord>());
                }
                
                List<GenericRecord> inputsForKey = inputsByKey.get(key);
                inputsForKey.add(deriveOutputFromInput(input));
            }
        }
        
        // The list of operations that will effect the changes required to the storage layer. 
        List<Object> operations = Lists.newArrayList();
        
        // Iterate over each key in the partition of the micro-batch.
        for (Map<String, Object> key : inputsByKey.keySet()) {
            // The input records for the key.
            List<GenericRecord> inputsForKey = inputsByKey.get(key);
            
//            System.out.println("inputsForKey:");
//            if (inputsForKey.size() > 0) {
//                System.out.println(Arrays.toString(inputsForKey.toArray()));
//            } else {
//                System.out.println("[empty]");
//            }
            
            // The existing storage layer records for the key.
            List<GenericRecord> outputsForKey = recordsFromOutputForKey(key);
            
//            System.out.println("outputsForKey:");
//            if (outputsForKey.size() > 0) {
//                System.out.println(Arrays.toString(outputsForKey.toArray()));
//            } else {
//                System.out.println("[empty]");
//            }
            
            // Get the operations required to apply to the storage layer so that the records
            // for the key are up to date. This depends on whether we are tracking history or
            // just overwriting old values with new. We append the list of operations for the key
            // to the list for the partition of the micro-batch, which allows us to batch
            // operations for higher throughput.
            if (doesTrackHistory()) {
                operations.addAll(operationsRequiredForHistory(inputsForKey, outputsForKey));
            }
            else {
                operations.addAll(operationsRequiredForNoHistory(inputsForKey, outputsForKey));
            }
        }
        
        // Apply the storage layer operations at the conclusion of processing all of the keys
        // for the partition of the micro-batch.
        applyOperations(operations);
        
        // Disconnect from the storage layer now that the partition of the micro-batch is complete.
        disconnect();
    }
    
    // The storage layer operations required to apply the inputs without history. This means that
    // new records are inserted, and existing records are updated but only if they have changed.
    private List<Object> operationsRequiredForNoHistory(List<GenericRecord> inputsForKey,
                                                        List<GenericRecord> outputsForKey)
    {
        // Sort inputs for key in reverse time order so that we can use the most recent version.
        Collections.sort(inputsForKey, Collections.reverseOrder(new TimestampComparator()));
        GenericRecord input = inputsForKey.get(0);
        Long inputTimestamp = (Long)input.get(getEffectiveFromFieldName());
        
        GenericRecord output = null;
        Long outputTimestamp = null;
        if (outputsForKey.size() > 0) {
            // When not tracking history there should be at most one output record per key.
            output = outputsForKey.get(0);
            outputTimestamp = (Long)output.get(getEffectiveFromFieldName());
        }
        
        // Input key seen for first time, so we insert.
        if (outputTimestamp == null) {
            input.put(getEffectiveFromFieldName(), inputTimestamp);
            input.put(getLastUpdatedFieldName(), currentTimestampString());
            input.put(OPERATION_FIELD_NAME, OPERATION_INSERT);
            outputsForKey.add(input);
        }
        // Input is a previous version of output, so we do nothing.
        else if (inputTimestamp < outputTimestamp) {
        }
        // Input is the same or newer version as output, so update only if changed.
        else if (inputTimestamp >= outputTimestamp && hasDifference(input, output)) {
            copyOverValueFields(input, output);
            output.put(getLastUpdatedFieldName(), currentTimestampString());
            output.put(OPERATION_FIELD_NAME, OPERATION_UPDATE);
        }
        
        return extractOperations(outputsForKey);
    }
    
    // The storage layer operations required to apply the inputs with history. This means that
    // all versions, as identified by its timestamp, is maintained. Each version contains the
    // timestamps that it is first effective and last effective, as well as a current flag to make
    // it more convenient to select the most recent version of each record. Past versions are
    // updated when they have been superseded to reflect their new effective to timestamp and
    // current flag.
    private List<Object> operationsRequiredForHistory(List<GenericRecord> inputsForKey,
                                                      List<GenericRecord> outputsForKey)
    {
        // Sort the existing output records for the key so we can traverse them in time order.
        Collections.sort(outputsForKey, new TimestampComparator());
        
//        System.out.println("outputsForKey sorted:");
//        if (outputsForKey.size() > 0) {
//            System.out.println(Arrays.toString(outputsForKey.toArray()));
//        } else {
//            System.out.println("[empty]");
//        }
        
        // Loop through each input of the key, traversing the existing output records, and
        // identifying where changes need to be made to the history.
        for (GenericRecord input : inputsForKey) {
            Long inputTimestamp = (Long)input.get(getEffectiveFromFieldName());
            
//            System.out.println("input:");
//            System.out.println(input);
//            System.out.println("inputTimestamp:");
//            System.out.println(inputTimestamp);
            
            // There was no existing record for the key, so we just insert the input record.
            if (outputsForKey.size() == 0) {
                input.put(getEffectiveFromFieldName(), inputTimestamp);
                input.put(getEffectiveToFieldName(), FAR_FUTURE_MS);
                input.put(getCurrentFlagFieldName(), CURRENT_FLAG_YES);
                input.put(getLastUpdatedFieldName(), currentTimestampString());
                input.put(OPERATION_FIELD_NAME, OPERATION_INSERT);
                outputsForKey.add(input);
                
//                System.out.println("----- Not found");
//                System.out.println("input:");
//                System.out.println(input);
            }
            
            // Iterate through each existing record of the key in time order, stopping when
            // have either corrected the history or gone all the way through it.
            for (int position = 0; position < outputsForKey.size(); position++) {
                GenericRecord output = outputsForKey.get(position);
                Long outputTimestamp = (Long)output.get(getEffectiveFromFieldName());
                GenericRecord previousOutput = null;
                Long previousOutputTimestamp = null;
                GenericRecord nextOutput = null;
                Long nextOutputTimestamp = null;

                if (position > 0) {
                   previousOutput = outputsForKey.get(position - 1);
                   previousOutputTimestamp = (Long)previousOutput.get(getEffectiveFromFieldName());
                }
                if (position + 1 < outputsForKey.size()) {
                    nextOutput = outputsForKey.get(position + 1);
                    nextOutputTimestamp = (Long)nextOutput.get(getEffectiveFromFieldName());
                }
                
//                System.out.println("output:");
//                System.out.println(output);
//                System.out.println("outputTimestamp:");
//                System.out.println(outputTimestamp);
//                System.out.println("previousOutput:");
//                System.out.println(previousOutput);
//                System.out.println("previousOutputTimestamp:");
//                System.out.println(previousOutputTimestamp);
//                System.out.println("nextOutput:");
//                System.out.println(nextOutput);
//                System.out.println("nextOutputTimestamp:");
//                System.out.println(nextOutputTimestamp);

                // There is an existing record for the same key and timestamp. It is possible that
                // the existing record is in the storage layer or is about to be added during this
                // micro-batch. Either way, we only update that record if it has changed.
                if (inputTimestamp == outputTimestamp && hasDifference(input, output)) {
                    copyOverValueFields(input, output);
                    output.put(getLastUpdatedFieldName(), currentTimestampString());
                    // If the record is being added during this micro-batch then we need to
                    // leave it tagged as an insert even though here we are updating its values.
                    if (!output.get(OPERATION_FIELD_NAME).equals(OPERATION_INSERT)) {
                        output.put(OPERATION_FIELD_NAME, OPERATION_UPDATE);
                    }
                    
//                    System.out.println("----- At the same time");
//                    System.out.println("output:");
//                    System.out.println(output);
                    
                    break;
                }
                // Before them all
                // -> Insert with ED just before first
                // The input record is timestamped before any existing record of the same key. In
                // this case there is no need to modify existing records, and we only have to insert
                // the input record as effective up until just prior to the first existing record.
                else if (previousOutputTimestamp == null && inputTimestamp < outputTimestamp) {
                    input.put(getEffectiveFromFieldName(), inputTimestamp);
                    input.put(getEffectiveToFieldName(), outputTimestamp - 1);
                    input.put(getCurrentFlagFieldName(), CURRENT_FLAG_NO);
                    input.put(getLastUpdatedFieldName(), currentTimestampString());
                    input.put(OPERATION_FIELD_NAME, OPERATION_INSERT);
                    outputsForKey.add(input);
                    
//                    System.out.println("----- Before them all");
//                    System.out.println("input:");
//                    System.out.println(input);
                    
                    break;
                }
                // The input record is timestamped with an existing record of the same key before it
                // and an existing record of the same key after it. We insert the input record
                // effective until just prior to the next existing record and we update the
                // previous existing record to be effective until just prior to the input record.
                else if (outputTimestamp != null && nextOutputTimestamp != null &&
                         inputTimestamp > outputTimestamp &&
                         inputTimestamp < nextOutputTimestamp)
                {
                    input.put(getEffectiveFromFieldName(), inputTimestamp);
                    input.put(getEffectiveToFieldName(), nextOutputTimestamp - 1);
                    input.put(getCurrentFlagFieldName(), CURRENT_FLAG_NO);
                    input.put(getLastUpdatedFieldName(), currentTimestampString());
                    input.put(OPERATION_FIELD_NAME, OPERATION_INSERT);
                    outputsForKey.add(input);
                    
//                    System.out.println("----- In the middle");
//                    System.out.println("input:");
//                    System.out.println(input);
                    
                    output.put(getEffectiveToFieldName(), inputTimestamp - 1);
                    output.put(getCurrentFlagFieldName(), CURRENT_FLAG_NO);
                    output.put(getLastUpdatedFieldName(), currentTimestampString());
                    if (!output.get(OPERATION_FIELD_NAME).equals(OPERATION_INSERT)) {
                        output.put(OPERATION_FIELD_NAME, OPERATION_UPDATE);
                    }
                    
//                    System.out.println("output:");
//                    System.out.println(output);
                    
                    break;
                }
                // The input record is arriving after all existing records of the same key. This
                // is the 'normal' case where data arrives in order. We insert the input record
                // effective until the far future, and we update the previous existing record
                // to be effective until just prior to the input record.
                else if (inputTimestamp > outputTimestamp && nextOutputTimestamp == null) {
                    input.put(getEffectiveFromFieldName(), inputTimestamp);
                    input.put(getEffectiveToFieldName(), FAR_FUTURE_MS);
                    input.put(getCurrentFlagFieldName(), CURRENT_FLAG_YES);
                    input.put(getLastUpdatedFieldName(), currentTimestampString());
                    input.put(OPERATION_FIELD_NAME, OPERATION_INSERT);
                    outputsForKey.add(input);
                    
//                    System.out.println("----- After them all");
//                    System.out.println("input:");
//                    System.out.println(input);
                    
                    output.put(getEffectiveToFieldName(), inputTimestamp - 1);
                    output.put(getCurrentFlagFieldName(), CURRENT_FLAG_NO);
                    output.put(getLastUpdatedFieldName(), currentTimestampString());
                    if (!output.get(OPERATION_FIELD_NAME).equals(OPERATION_INSERT)) {
                        output.put(OPERATION_FIELD_NAME, OPERATION_UPDATE);
                    }
                    
//                    System.out.println("output:");
//                    System.out.println(output);
                    
                    break;
                }
            }
            
            // With the existing record list now updated with the input record we need to
            // re-sort the existing record list to traverse it again for the next key.
            // TODO: don't re-sort if there was no change
            // TODO: don't re-sort if there are no more input records for the key
            Collections.sort(outputsForKey, new TimestampComparator());
            
//            System.out.println("outputsForKey re-sorted:");
//            if (outputsForKey.size() > 0) {
//                System.out.println(Arrays.toString(outputsForKey.toArray()));
//            } else {
//                System.out.println("[empty]");
//            }
        }
        
        return extractOperations(outputsForKey);
    }
    
    // The system timestamp string. Used for persisting as the last updated timestamp.
    private String currentTimestampString() {
        return new Date(System.currentTimeMillis()).toString();
    }
    
    // The storage key of the given input record. A key can have multiple fields each with different
    // data types. A field name must be a string, however.
    private Map<String, Object> keyFromInput(GenericRecord input) {
        Map<String, Object> key = Maps.newHashMap();
        for (String keyFieldName : getKeyFieldNames()) {
            key.put(keyFieldName, deriveOutputField(input, keyFieldName));
        }
        
        return key;
    }
    
    // Whether the two records have any difference in their value fields.
    private boolean hasDifference(GenericRecord first, GenericRecord second) {
        boolean differenceFound = false;
        
        for (Field field : first.getSchema().getFields()) {
            String columnName = field.name();
            Object firstValue = first.get(columnName);
            Object secondValue = second.get(columnName);
            if (getValueFieldNames().contains(columnName) &&
                firstValue != null && secondValue != null &&
                !firstValue.equals(secondValue))
            {
                differenceFound = true;
            }
            
            if ((firstValue != null && secondValue == null) ||
                (firstValue == null && secondValue != null))
            {
                differenceFound = true;
            }
        }
        
        return differenceFound;
    }
    
    // Updates the second record's value fields with those of the first record.
    private void copyOverValueFields(GenericRecord from, GenericRecord to) {
        for (Field field : from.getSchema().getFields()) {
            String fieldName = field.name();
            if (getValueFieldNames().contains(fieldName)) {
                to.put(fieldName, from.get(fieldName));
            }
        }
    }
    
    // Transforms the input record into an output record with the storage layer schema.
    private GenericRecord deriveOutputFromInput(GenericRecord input) {
        Schema schema = outputSchema();
        
        GenericRecord output = new GenericData.Record(schema);
        
        for (Field field : schema.getFields()) {
            output.put(field.name(), deriveOutputField(input, field.name()));
        }
        
        return output;
    }
    
    // Comparator for sorting Avro records by the storage layer timestamp field.
    public class TimestampComparator implements Comparator<GenericRecord> {
        @Override
        public int compare(GenericRecord rr1, GenericRecord rr2) {
            Long ts1 = (Long)rr1.get(getEffectiveFromFieldName());
            Long ts2 = (Long)rr2.get(getEffectiveFromFieldName());
            if      (ts1 < ts2) return -1;
            else if (ts1 > ts2) return 1;
            else return 0;
        }
    }
    
}
