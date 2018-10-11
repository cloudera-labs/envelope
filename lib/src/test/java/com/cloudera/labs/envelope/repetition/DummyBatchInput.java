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

package com.cloudera.labs.envelope.repetition;

import com.cloudera.labs.envelope.input.BatchInput;
import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DummyBatchInput implements BatchInput {

  private static final int NUM_ROWS = 100;

  private long counter = 0;
  private int numRows = NUM_ROWS;

  private StructType schema = new StructType(
      new StructField[] {
          new StructField("id", DataTypes.LongType, false, Metadata.empty())
      });

  @Override
  public Dataset<Row> read() throws Exception {
    Dataset<Row> ds = Contexts.getSparkSession().range(counter, counter + numRows).map(new LongToRowFunction(),
        RowEncoder.apply(schema));
    counter = counter+numRows;
    return ds;
  }

  @Override
  public void configure(Config config) {
    if (config.hasPath("numrows")) {
      numRows = config.getInt("numrows");
    }
  }

  private static class LongToRowFunction implements MapFunction<Long, Row> {
    @Override
    public Row call(Long value) throws Exception {
      return RowFactory.create(value);
    }
  }

}
