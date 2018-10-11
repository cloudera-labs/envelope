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

package com.cloudera.labs.envelope.run;

import com.cloudera.labs.envelope.input.BatchInput;
import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;

public class DummyInput implements BatchInput {
  
  private int numPartitions;
  
  @Override
  public void configure(Config config) {
    numPartitions = config.getInt("starting.partitions");
  }

  @Override
  public Dataset<Row> read() throws Exception {
    Dataset<Row> df = Contexts.getSparkSession()
        .range(numPartitions * 10)
        .repartition(numPartitions)
        .map(new LongToRowFunction(), 
            RowEncoder.apply(DataTypes.createStructType(
                Lists.newArrayList(
                    DataTypes.createStructField("value", DataTypes.LongType, true),
                    DataTypes.createStructField("modulo", DataTypes.LongType, true))
                )));
    return df;
  }

  @SuppressWarnings("serial")
  private static class LongToRowFunction implements MapFunction<Long, Row> {
    @Override
    public Row call(Long value) throws Exception {
      return RowFactory.create(value, value % 5);
    }
  }
  
}
