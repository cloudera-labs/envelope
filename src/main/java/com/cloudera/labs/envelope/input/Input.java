package com.cloudera.labs.envelope.input;

import com.typesafe.config.Config;

public interface Input {
    
    void configure(Config config);
    
}
