package com.cloudera.fce.envelope;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.cloudera.fce.envelope.deriver.Deriver;
import com.cloudera.fce.envelope.planner.PlannedRecord;
import com.cloudera.fce.envelope.planner.Planner;
import com.cloudera.fce.envelope.storage.StorageTable;
import com.cloudera.fce.envelope.utils.PropertiesUtils;
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
        
        derivedRecords.foreachPartition(new VoidFunction<Iterator<GenericRecord>>() {
            @Override
            public void call(Iterator<GenericRecord> arrivingIterator) throws Exception {
                StorageTable storageTable = StorageTable.storageTableFor(props);
                storageTable.connect();
                
                List<GenericRecord> arriving = Lists.newArrayList(arrivingIterator);
                List<GenericRecord> existing = storageTable.getExistingForArriving(arriving);
                RecordModel recordModel = storageTable.getRecordModel();
                
                Planner planner = Planner.plannerFor(props);
                List<PlannedRecord> planned = planner.planOperations(arriving, existing, recordModel);
                storageTable.applyPlannedOperations(planned);
                
                storageTable.disconnect();
            }
        });
    }
    
    public static List<Flow> flowsFor(Properties props) {
        List<Flow> flows = Lists.newArrayList();
        
        List<String> flowNames = PropertiesUtils.propertyAsList(props, "flow.names");
        
        for (String flowName : flowNames) {
            Properties flowProps = PropertiesUtils.prefixProperties(props, "flow." + flowName + ".");
            Flow flow = new Flow(flowProps);
            flows.add(flow);
        }
        
        return flows;
    }
    
}
