package com.cloudera.labs.envelope.output.bulk;

import java.util.List;
import java.util.Set;

import org.apache.kudu.client.shaded.com.google.common.collect.Sets;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;

import com.cloudera.labs.envelope.plan.MutationType;
import com.typesafe.config.Config;

import scala.Tuple2;

public class HiveOutput extends BulkWriteOutput {
    
    public final static String TABLE_CONFIG_NAME = "table";
    public final static String PARTITION_BY_CONFIG_NAME = "partition.by";

    public HiveOutput(Config config) {
        super(config);
        
        if (!config.hasPath(TABLE_CONFIG_NAME)) {
            throw new RuntimeException("Hive output requires '" + TABLE_CONFIG_NAME + "' property");
        }
    }

    @Override
    public void applyMutations(List<Tuple2<MutationType, DataFrame>> planned) throws Exception {        
        for (Tuple2<MutationType, DataFrame> plan : planned) {
            MutationType mutationType = plan._1();
            DataFrame mutation = plan._2();
            DataFrameWriter writer = mutation.write();
            
            if (hasPartitionColumns()) {
                writer = writer.partitionBy(getPartitionColumns());
            }

            switch (mutationType) {
                case INSERT:
                    writer = writer.mode(SaveMode.Append);
                    break;
                case OVERWRITE:
                    writer = writer.mode(SaveMode.Overwrite);
                    break;
                default:
                    throw new RuntimeException("Hive output does not support mutation type: " + mutationType);
            }
            
            writer.saveAsTable(getTableName());
        }
    }

    @Override
    public Set<MutationType> getSupportedMutationTypes() {
        return Sets.newHashSet(MutationType.INSERT, MutationType.OVERWRITE);
    }
    
    private boolean hasPartitionColumns() {
        return config.hasPath(PARTITION_BY_CONFIG_NAME);
    }
    
    private String[] getPartitionColumns() {
        return (String[])config.getList(PARTITION_BY_CONFIG_NAME).toArray();
    }
    
    private String getTableName() {
        return config.getString(TABLE_CONFIG_NAME);
    }

}
