package com.cloudera.labs.envelope.derive;

import java.util.Iterator;
import java.util.Map;

import org.apache.spark.sql.DataFrame;

import com.typesafe.config.Config;

public class PassthroughDeriver extends Deriver {

    public PassthroughDeriver(Config config) {
        super(config);
    }

    @Override
    public DataFrame derive(Map<String, DataFrame> dependencies) throws Exception {
        if (dependencies.isEmpty()) {
            throw new RuntimeException("Passthrough deriver requires at least one dependency");
        }
        
        Iterator<DataFrame> dependencyIterator = dependencies.values().iterator();
        
        DataFrame unioned = dependencyIterator.next();
        while (dependencyIterator.hasNext()) {
            unioned = unioned.unionAll(dependencyIterator.next());
        }
        
        return unioned;
    }

}
