package com.cloudera.labs.envelope;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.derive.Deriver;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRecord;
import com.cloudera.labs.envelope.plan.Planner;
import com.cloudera.labs.envelope.store.StorageSystems;
import com.cloudera.labs.envelope.store.StorageTable;
import com.cloudera.labs.envelope.utils.PropertiesUtils;
import com.cloudera.labs.envelope.utils.RecordUtils;
import com.cloudera.labs.envelope.utils.SparkSQLAvroUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A flow processes a translated stream, including transformation and storage. Each micro-batch can
 * run multiple flows.
 */
@SuppressWarnings("serial")
public class Flow implements Serializable {
    
    private Properties props;
    
    public Flow(Properties props) {
        this.props = props;
    }
    
    // For this flow, derive the storage records from the stream and lookup. 
    public void runFlow(DataFrame stream, Map<String, DataFrame> lookups) throws Exception {
        Deriver deriver = Deriver.deriverFor(props);
        DataFrame derivedDataFrame = deriver.derive(stream, lookups);
        JavaRDD<Row> derivedRows = derivedDataFrame.javaRDD();
        JavaRDD<GenericRecord> derivedRecords = SparkSQLAvroUtils.recordsForRows(derivedRows);
        
        runFlow(derivedRecords);
    }
    
    // For this flow, plan the mutations and apply them to the storage.
    public void runFlow(JavaRDD<GenericRecord> storageRecords) throws Exception {
        // Co-locate all records of the same key in the same RDD partition, if the planner requires it.
        if (Planner.plannerFor(props).requiresKeyColocation()) {
            storageRecords = colocateByKey(storageRecords, props, RecordModel.recordModelFor(props));
        }
        
        storageRecords.foreachPartition(new VoidFunction<Iterator<GenericRecord>>() {
            @Override
            public void call(Iterator<GenericRecord> arrivingIterator) throws Exception {
                RecordModel recordModel = RecordModel.recordModelFor(props);
                List<GenericRecord> arriving = Lists.newArrayList(arrivingIterator);
                
                if (arriving.size() == 0) return;
                
                Planner planner = Planner.plannerFor(props);
                StorageTable storageTable = StorageSystems.tableFor(props);
                validatePlannerStorageCompatibility(planner, storageTable);
                
                List<PlannedRecord> planned;
                if (planner.requiresExistingRecords()) {
                    List<GenericRecord> existing = Lists.newArrayList();
                    
                    Schema keySchema = RecordUtils.subsetSchema(arriving.get(0).getSchema(), recordModel.getKeyFieldNames());
                    for (GenericRecord arrived : arriving) {
                        GenericRecord key = RecordUtils.subsetRecord(arrived, keySchema);
                        existing.addAll(storageTable.getExistingForFilter(key));
                    }
                    
                    planned = planner.planMutations(arriving, existing, recordModel);
                }
                else {
                    planned = planner.planMutations(arriving, recordModel);
                }
                
                storageTable.applyPlannedMutations(planned);
            }
        });
    }
    
    private void validatePlannerStorageCompatibility(Planner planner, StorageTable storageTable) {
        Set<MutationType> storageMTs = storageTable.getSupportedMutationTypes();
        Set<MutationType> plannerMTs = planner.getEmittedMutationTypes();
        
        for (MutationType planMT : plannerMTs) {
            if (!storageMTs.contains(planMT)) {
                throw new RuntimeException("Incompatible planner (" + planner.getClass() + ") and storage (" + storageTable.getClass() + ").");
            }
        }
    }
    
    // Co-locate all records for the same key in the same RDD partition.
    private JavaRDD<GenericRecord> colocateByKey(JavaRDD<GenericRecord> records, Properties props, final RecordModel recordModel)
    {
        return records
            // Group together all records for the same key
            .groupBy(new Function<GenericRecord, GenericRecord>() {
                Schema schema;
                @Override
                public GenericRecord call(GenericRecord record) throws Exception {
                    if (schema == null) {
                        schema = RecordUtils.subsetSchema(record.getSchema(), recordModel.getKeyFieldNames());
                    }
                    return RecordUtils.subsetRecord(record, schema);
                }
            })
            // Remove the key
            .values()
            // Flatten out the grouped values of the keys
            .flatMap(new FlatMapFunction<Iterable<GenericRecord>, GenericRecord>() {
                @Override
                public Iterable<GenericRecord> call(Iterable<GenericRecord> keyedRecords) {
                    return keyedRecords;
                }
            });
    }
    
    public boolean hasDeriver() {
        return props.containsKey("deriver");
    }
    
    public static Set<Flow> flowsFor(Properties props) {
        Set<Flow> flows = Sets.newHashSet();
        
        List<String> flowNames = PropertiesUtils.propertyAsList(props, "flows");
        
        for (String flowName : flowNames) {
            Properties flowProps = PropertiesUtils.prefixProperties(props, "flow." + flowName + ".");
            Flow flow = new Flow(flowProps);
            flows.add(flow);
        }
        
        return flows;
    }
    
}
