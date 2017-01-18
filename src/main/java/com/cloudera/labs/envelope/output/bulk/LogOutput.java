package com.cloudera.labs.envelope.output.bulk;

import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.plan.MutationType;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

public class LogOutput extends BulkOutput {
    
    public static final String DELIMITER_CONFIG_NAME = "delimiter";
    public static final String LOG_LEVEL_CONFIG_NAME = "level";
    
    private static Logger LOG = LoggerFactory.getLogger(LogOutput.class);

    public LogOutput(Config config) {
        super(config);
    }

    @SuppressWarnings("serial")
    @Override
    public void applyMutations(List<Tuple2<MutationType, DataFrame>> planned) throws Exception {
        for (Tuple2<MutationType, DataFrame> mutation : planned) {
            MutationType mutationType = mutation._1();
            DataFrame mutationDF = mutation._2();
            
            if (mutationType.equals(MutationType.INSERT)) {
                mutationDF.javaRDD().foreach(new VoidFunction<Row>() {
                    private Joiner joiner;
                    
                    @Override
                    public void call(Row mutation) throws Exception {
                        if (joiner == null) {
                            String delimiter = getDelimiter();
                            joiner = Joiner.on(delimiter);
                        }
                        
                        String logLevel = getLogLevel();
                        
                        List<Object> values = Lists.newArrayList();
                        
                        for (int fieldIndex = 0; fieldIndex < mutation.size(); fieldIndex++) {
                            values.add(mutation.get(fieldIndex));
                        }
                        String log = joiner.join(values);
                        
                        switch (logLevel) {
                            case "TRACE":
                                LOG.trace(log);
                                break;
                            case "DEBUG":
                                LOG.debug(log);
                                break;
                            case "INFO":
                                LOG.info(log);
                                break;
                            case "WARN":
                                LOG.warn(log);
                                break;
                            case "ERROR":
                                LOG.error(log);
                                break;
                            default:
                                throw new RuntimeException("Invalid log level: " + logLevel);
                        }
                    }
                });
            }
        }
    }

    @Override
    public Set<MutationType> getSupportedMutationTypes() {
        return Sets.newHashSet(MutationType.INSERT);
    }
    
    private String getDelimiter() {
        if (!config.hasPath(DELIMITER_CONFIG_NAME)) return ",";
        
        return config.getString(DELIMITER_CONFIG_NAME);
    }
    
    private String getLogLevel() {
        if (!config.hasPath(LOG_LEVEL_CONFIG_NAME)) return "INFO";
        
        return config.getString(LOG_LEVEL_CONFIG_NAME).toUpperCase();
    }

}
