package com.cloudera.labs.envelope.input;

import org.apache.spark.sql.DataFrame;

import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;

public class FileSystemInput implements BatchInput {

  public static final String FORMAT_CONFIG_NAME = "format";
  public static final String PATH_CONFIG_NAME = "path";

  private Config config;

  @Override
  public void configure(Config config) {
    this.config = config;

    if (!config.hasPath(FORMAT_CONFIG_NAME)) {
      throw new RuntimeException("Filesystem input requires '" + FORMAT_CONFIG_NAME + "' config");
    }
    if (!config.hasPath(PATH_CONFIG_NAME)) {
      throw new RuntimeException("Filesystem input requires '" + PATH_CONFIG_NAME + "' config");
    }
  }

  @Override
  public DataFrame read() throws Exception {
    String format = config.getString(FORMAT_CONFIG_NAME);
    String path = config.getString(PATH_CONFIG_NAME);

    DataFrame fs = null;

    switch (format) {
      case "parquet":
        fs = Contexts.getSQLContext().read().parquet(path);
        break;
      case "avro":
        fs = Contexts.getSQLContext().read().format("com.databricks.spark.avro").load(path);
        break;
      case "json":
        fs = Contexts.getSQLContext().read().json(path);
        break;
      default:
        throw new RuntimeException("Filesystem input format not supported: " + format);
    }

    return fs;
  }

}
