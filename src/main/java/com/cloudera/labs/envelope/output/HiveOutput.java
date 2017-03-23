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

import org.apache.kudu.client.shaded.com.google.common.collect.Sets;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;

import com.cloudera.labs.envelope.plan.MutationType;
import com.typesafe.config.Config;

import scala.Tuple2;

public class HiveOutput implements BulkOutput {

  public final static String TABLE_CONFIG_NAME = "table";
  public final static String PARTITION_BY_CONFIG_NAME = "partition.by";

  private Config config;

  @Override
  public void configure(Config config) {
    this.config = config;

    if (!config.hasPath(TABLE_CONFIG_NAME)) {
      throw new RuntimeException("Hive output requires '" + TABLE_CONFIG_NAME + "' property");
    }
  }

  @Override
  public void applyBulkMutations(List<Tuple2<MutationType, DataFrame>> planned) throws Exception {    
    for (Tuple2<MutationType, DataFrame> plan : planned) {
      MutationType mutationType = plan._1();
      DataFrame mutation = plan._2();
      DataFrameWriter writer = mutation.write();

      if (hasPartitionColumns()) {
        writer = writer.partitionBy(getPartitionColumns());
      }

      switch (mutationType) {
        case INSERT:
          writer = writer.mode(SaveMode.Append);
          break;
        case OVERWRITE:
          writer = writer.mode(SaveMode.Overwrite);
          break;
        default:
          throw new RuntimeException("Hive output does not support mutation type: " + mutationType);
      }

      writer.saveAsTable(getTableName());
    }
  }

  @Override
  public Set<MutationType> getSupportedBulkMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT, MutationType.OVERWRITE);
  }

  private boolean hasPartitionColumns() {
    return config.hasPath(PARTITION_BY_CONFIG_NAME);
  }

  private String[] getPartitionColumns() {
    return (String[])config.getList(PARTITION_BY_CONFIG_NAME).toArray();
  }

  private String getTableName() {
    return config.getString(TABLE_CONFIG_NAME);
  }

}
