package com.cloudera.fce.envelope;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
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

import com.cloudera.fce.envelope.deriver.Deriver;
import com.cloudera.fce.envelope.planner.OperationType;
import com.cloudera.fce.envelope.planner.PlannedRecord;
import com.cloudera.fce.envelope.planner.Planner;
import com.cloudera.fce.envelope.storage.StorageTable;
import com.cloudera.fce.envelope.storage.StorageSystems;
import com.cloudera.fce.envelope.utils.PropertiesUtils;
import com.cloudera.fce.envelope.utils.RecordUtils;
import com.cloudera.fce.envelope.utils.SparkSQLAvroUtils;
import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class Flow implements Serializable {
    
    private Properties props;
    
    public Flow(Properties props) {
        this.props = props;
    }
    
    public void runFlow(DataFrame batch) throws Exception {
        DataFrame derivedDataFrame = Deriver.deriverFor(props).derive(batch);
        JavaRDD<Row> derivedRows = derivedDataFrame.javaRDD();
        JavaRDD<GenericRecord> derivedRecords = SparkSQLAvroUtils.recordsForRows(derivedRows);
        
        if (Planner.plannerFor(props).requiresKeyColocation()) {
            derivedRecords = colocateByKey(derivedRecords, props, RecordModel.recordModelFor(props));
        }
        
        derivedRecords.foreachPartition(new VoidFunction<Iterator<GenericRecord>>() {
            @Override
            public void call(Iterator<GenericRecord> arrivingIterator) throws Exception {
                RecordModel recordModel = RecordModel.recordModelFor(props);
                List<GenericRecord> arriving = Lists.newArrayList(arrivingIterator);
                
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
                    
                    planned = planner.planOperations(arriving, existing, recordModel);
                }
                else {
                    planned = planner.planOperations(arriving, recordModel);
                }
                
                storageTable.applyPlannedOperations(planned);
            }
        });
    }
    
    private void validatePlannerStorageCompatibility(Planner planner, StorageTable storageTable) {
        Set<OperationType> storageOTs = storageTable.getSupportedOperationTypes();
        Set<OperationType> plannerOTs = planner.getEmittedOperationTypes();
        
        for (OperationType planOT : plannerOTs) {
            if (!storageOTs.contains(planOT)) {
                throw new RuntimeException("Incompatible planner (" + planner.getClass() + ") and storage (" + storageTable.getClass() + ").");
            }
        }
    }
    
    private JavaRDD<GenericRecord> colocateByKey(JavaRDD<GenericRecord> records, Properties props,
            final RecordModel recordModel)
    {
        return records
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
            .values()
            .flatMap(new FlatMapFunction<Iterable<GenericRecord>, GenericRecord>() {
                @Override
                public Iterable<GenericRecord> call(Iterable<GenericRecord> keyedRecords) {
                    return keyedRecords;
                }
            });
    }
    
    public static List<Flow> flowsFor(Properties props) {
        List<Flow> flows = Lists.newArrayList();
        
        List<String> flowNames = PropertiesUtils.propertyAsList(props, "flows");
        
        for (String flowName : flowNames) {
            Properties flowProps = PropertiesUtils.prefixProperties(props, "flow." + flowName + ".");
            Flow flow = new Flow(flowProps);
            flows.add(flow);
        }
        
        return flows;
    }
    
}
