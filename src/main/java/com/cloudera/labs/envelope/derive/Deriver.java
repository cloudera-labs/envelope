package com.cloudera.labs.envelope.derive;

import java.util.Map;

import org.apache.spark.sql.DataFrame;

import com.typesafe.config.Config;

public interface Deriver {

  void configure(Config config);

  DataFrame derive(Map<String, DataFrame> dependencies) throws Exception;

}
