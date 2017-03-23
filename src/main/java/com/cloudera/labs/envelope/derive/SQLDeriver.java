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
package com.cloudera.labs.envelope.derive;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.typesafe.config.Config;

/**
 * An input implementaton for Spark SQL.
 */
public class SQLDeriver implements Deriver {

  public static final String QUERY_LITERAL_CONFIG_NAME = "query.literal";
  public static final String QUERY_FILE_CONFIG_NAME = "query.file";

  private Config config;

  @Override
  public void configure(Config config) {
    this.config = config;
  }

  @Override
  public DataFrame derive(Map<String, DataFrame> dependencies) throws Exception {
    String query;

    if (config.hasPath(QUERY_LITERAL_CONFIG_NAME)) {
      query = config.getString(QUERY_LITERAL_CONFIG_NAME);
    }
    else if (config.hasPath(QUERY_FILE_CONFIG_NAME)) {
      query = hdfsFileAsString(config.getString(QUERY_FILE_CONFIG_NAME));
    }
    else {
      throw new RuntimeException("SQL deriver query not provided. Use '" + QUERY_LITERAL_CONFIG_NAME + "' or '" + QUERY_FILE_CONFIG_NAME + "'.");
    }

    SQLContext sqlc = Contexts.getSQLContext();
    DataFrame derived = sqlc.sql(query);

    return derived;
  }

  private String hdfsFileAsString(String hdfsFile) throws Exception {
    String contents = null;

    FileSystem fs = FileSystem.get(new Configuration());
    InputStream stream = fs.open(new Path(hdfsFile));
    InputStreamReader reader = new InputStreamReader(stream, Charsets.UTF_8);
    contents = CharStreams.toString(reader);
    reader.close();
    stream.close();

    return contents;
  }

}
