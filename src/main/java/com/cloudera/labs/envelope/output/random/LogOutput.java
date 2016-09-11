package com.cloudera.labs.envelope.output.random;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

public class LogOutput extends RandomWriteOutput {
    
    public static final String DELIMITER_CONFIG_NAME = "log.delimiter";
    public static final String LOG_LEVEL_CONFIG_NAME = "log.level";
    
    private Joiner joiner;
    
    private static Logger LOG = LoggerFactory.getLogger(LogOutput.class);

    public LogOutput(Config config) {
        super(config);
    }

    @Override
    public void applyMutations(List<PlannedRow> planned) throws Exception {
        if (joiner == null) {
            String delimiter = getDelimiter();
            joiner = Joiner.on(delimiter);
        }
        
        String logLevel = getLogLevel();
        
        List<Object> values = Lists.newArrayList();
        
        for (PlannedRow plan : planned) {
            if (plan.getMutationType().equals(MutationType.INSERT)) {
                values.clear();
                for (int fieldIndex = 0; fieldIndex < plan.getRow().size(); fieldIndex++) {
                    values.add(plan.getRow().get(fieldIndex));
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
