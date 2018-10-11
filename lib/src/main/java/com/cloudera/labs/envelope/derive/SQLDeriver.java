/*
 * Copyright (c) 2015-2018, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.cloudera.labs.envelope.validate.FilesystemPathAccessibleValidation;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Execute Spark SQL on Datasets.
 */
public class SQLDeriver implements Deriver, ProvidesAlias, ProvidesValidations {

  public static final String QUERY_LITERAL_CONFIG_NAME = "query.literal";
  public static final String QUERY_FILE_CONFIG_NAME = "query.file";
  public static final String PARAMETER_PREFIX_CONFIG_NAME = "parameter";

  private Config config;

  @Override
  public void configure(Config config) {
    this.config = config;
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    String query;

    if (config.hasPath(QUERY_LITERAL_CONFIG_NAME)) {
      query = config.getString(QUERY_LITERAL_CONFIG_NAME);
    }
    else {
      query = hdfsFileAsString(config.getString(QUERY_FILE_CONFIG_NAME));
    }

    if (config.hasPath(PARAMETER_PREFIX_CONFIG_NAME)) {
      query = resolveParameters(query, config.getConfig(PARAMETER_PREFIX_CONFIG_NAME));
    }

    Dataset<Row> derived = Contexts.getSparkSession().sql(query);

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

  private String resolveParameters(String query, Config parameterConfig) {
    for (String parameterName : parameterConfig.root().keySet()) {
      String parameterValue = parameterConfig.getAnyRef(parameterName).toString();

      query = query.replaceAll(Pattern.quote("${" + parameterName + "}"), parameterValue);
    }

    return query;
  }

  @Override
  public String getAlias() {
    return "sql";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .exactlyOnePathExists(ConfigValueType.STRING, QUERY_FILE_CONFIG_NAME, QUERY_LITERAL_CONFIG_NAME)
        .handlesOwnValidationPath(PARAMETER_PREFIX_CONFIG_NAME)
        .add(new FilesystemPathAccessibleValidation(QUERY_FILE_CONFIG_NAME))
        .build();
  }
  
}
