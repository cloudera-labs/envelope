package com.cloudera.labs.envelope.utils;

import java.io.File;
import java.util.regex.Pattern;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ConfigUtils {

    public static Config configFromPath(String path) {
        File configFile = new File(path);
        Config config = ConfigFactory.parseFile(configFile);
                
        return config;
    }
    
    public static Config applySubstitutions(Config config, String substitutionsString) {
        String[] substitutions = substitutionsString.split(Pattern.quote(","));
        
        for (String substitution : substitutions) {
            Config substitutionConfig = ConfigFactory.parseString(substitution);
            config = config.withFallback(substitutionConfig);
        }
        
        Config resolvedConfig = config.resolve();
        
        return resolvedConfig;
    }
    
}
