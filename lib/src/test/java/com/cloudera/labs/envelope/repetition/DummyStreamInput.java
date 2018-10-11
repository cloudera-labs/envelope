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

import com.cloudera.labs.envelope.input.StreamInput;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import java.util.Queue;

public class DummyStreamInput implements StreamInput {

  private static final Logger LOG = LoggerFactory.getLogger(DummyStreamInput.class);
  private static final int ROWS_PER_BATCH = 100;

  private long counter = 0;
  private int rowsPerBatch = ROWS_PER_BATCH;

  private StructType schema = new StructType(
      new StructField[] {
          new StructField("id", DataTypes.LongType, false, Metadata.empty())
      });

  @Override
  public void configure(Config config) {
    if (config.hasPath("perbatch")) {
      rowsPerBatch = config.getInt("perbatch");
    }
  }

  @Override
  public JavaDStream<Long> getDStream() throws Exception {
    List<Long> list = Lists.newArrayList();
    for (int i = 0; i < rowsPerBatch; i++) {
      list.add(counter++);
    }
    JavaRDD<Long> longs = Contexts.getJavaStreamingContext().sparkContext().parallelize(list);
    Queue<JavaRDD<Long>> queue = Queues.newLinkedBlockingQueue();
    queue.add(longs);
    LOG.info("Created stream queue with {} rows", list.size());
    return Contexts.getJavaStreamingContext().queueStream(queue, true);
  }

  @Override
  public PairFunction<?, ?, ?> getPrepareFunction() {
    return new PairFunction<Long, Void, Row>() {
      @Override
      public Tuple2<Void, Row> call(Long aLong) throws Exception {
        return new Tuple2<>(null, (Row)new RowWithSchema(schema, aLong));
      }
    };
  }
  
}
