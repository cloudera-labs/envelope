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
package com.cloudera.labs.envelope.output;

import java.util.List;
import java.util.Set;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.cloudera.labs.envelope.plan.MutationType;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

public class FileSystemOutput implements BulkOutput {

  public final static String FORMAT_CONFIG_NAME = "format";
  public final static String PATH_CONFIG_NAME = "path";

  private Config config;

  @Override
  public void configure(Config config) {
    this.config = config;

    if (!config.hasPath(FORMAT_CONFIG_NAME)) {
      throw new RuntimeException("Filesystem output requires '" + FORMAT_CONFIG_NAME + "' property");
    }
    if (!config.hasPath(PATH_CONFIG_NAME)) {
      throw new RuntimeException("Filesystem output requires '" + PATH_CONFIG_NAME + "' property");
    }
  }

  @Override
  public void applyBulkMutations(List<Tuple2<MutationType, Dataset<Row>>> planned) throws Exception {
    for (Tuple2<MutationType, Dataset<Row>> plan : planned) {
      MutationType mutationType = plan._1();
      Dataset<Row> mutation = plan._2();

      String format = config.getString(FORMAT_CONFIG_NAME);
      String path = config.getString(PATH_CONFIG_NAME);

      DataFrameWriter<Row> writer = mutation.write();
      switch (mutationType) {
        case INSERT:
          writer = writer.mode(SaveMode.Append);
          break;
        case OVERWRITE:
          writer = writer.mode(SaveMode.Overwrite);
          break;
        default:
          throw new RuntimeException("Filesystem output does not support mutation type: " + mutationType);
      }

      switch (format) {
        case "parquet":
          writer.parquet(path);
          break;
        default:
          throw new RuntimeException("Filesystem output does not support file format: " + format);
      }
    }
  }

  @Override
  public Set<MutationType> getSupportedBulkMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT, MutationType.OVERWRITE);
  }

}
