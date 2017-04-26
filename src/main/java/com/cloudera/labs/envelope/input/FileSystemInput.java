/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.input;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;

public class FileSystemInput implements BatchInput {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemInput.class);

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
  public Dataset<Row> read() throws Exception {
    String format = config.getString(FORMAT_CONFIG_NAME);
    String path = config.getString(PATH_CONFIG_NAME);

    Dataset<Row> fs = null;

    switch (format) {
      case "parquet":
        LOG.debug("Reading Parquet: {}", path);
        fs = Contexts.getSparkSession().read().parquet(path);
        break;
      case "json":
        LOG.debug("Reading JSON: {}", path);
        fs = Contexts.getSparkSession().read().json(path);
        break;
      default:
        throw new RuntimeException("Filesystem input format not supported: " + format);
    }

    return fs;
  }

}
