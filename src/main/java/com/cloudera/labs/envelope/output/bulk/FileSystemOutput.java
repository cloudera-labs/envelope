package com.cloudera.labs.envelope.output.bulk;

import java.util.List;
import java.util.Set;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;

import com.cloudera.labs.envelope.plan.MutationType;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

public class FileSystemOutput extends BulkWriteOutput {
    
    public final static String FORMAT_CONFIG_NAME = "format";
    public final static String PATH_CONFIG_NAME = "path";

    public FileSystemOutput(Config config) {
        super(config);
        
        if (!config.hasPath(FORMAT_CONFIG_NAME)) {
            throw new RuntimeException("Filesystem output requires '" + FORMAT_CONFIG_NAME + "' property");
        }
        if (!config.hasPath(PATH_CONFIG_NAME)) {
            throw new RuntimeException("Filesystem output requires '" + PATH_CONFIG_NAME + "' property");
        }
    }
    
    @Override
    public void applyMutations(List<Tuple2<MutationType, DataFrame>> planned) throws Exception {
        for (Tuple2<MutationType, DataFrame> plan : planned) {
            MutationType mutationType = plan._1();
            DataFrame mutation = plan._2();
            
            String format = config.getString(FORMAT_CONFIG_NAME);
            String path = config.getString(PATH_CONFIG_NAME);
            
            DataFrameWriter writer = mutation.write();
            switch (mutationType) {
                case INSERT:
                    writer = writer.mode(SaveMode.Append);
                    break;
                case OVERWRITE:
                    writer = writer.mode(SaveMode.Overwrite);
                    break;
                default:
                    throw new RuntimeException("Filesystem output does not support mutation type: " + mutationType);
            }
            
            switch (format) {
                case "parquet":
                    writer.parquet(path);
                    break;
                case "avro":
                    writer.format("com.databricks.spark.avro").save(path);
                    break;
                default:
                    throw new RuntimeException("Filesystem output does not support file format: " + format);
            }
        }
    }

    @Override
    public Set<MutationType> getSupportedMutationTypes() {
        return Sets.newHashSet(MutationType.INSERT, MutationType.OVERWRITE);
    }

}
