package com.cloudera.labs.envelope.output;

import com.typesafe.config.Config;

public interface Output {

  void configure(Config config);

}
