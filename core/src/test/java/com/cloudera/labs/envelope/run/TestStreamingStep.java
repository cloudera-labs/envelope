/*
 * Copyright (c) 2015-2019, Cloudera, Inc. All Rights Reserved.
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

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.cloudera.labs.envelope.input.StreamInput;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestStreamingStep {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testConfigure() {
    Config config = ConfigFactory.empty()
        .withValue(StreamingStep.INPUT_TYPE, ConfigValueFactory.fromMap(new HashMap<String, Object>()))
        .withValue("input.type",
          ConfigValueFactory.fromAnyRef("com.cloudera.labs.envelope.run.TestStreamingStep$PrepartitionedStreamInput"))
        .withValue("input.translator", ConfigValueFactory.fromMap(new HashMap<String, Object>())
          .withValue("type", ConfigValueFactory.fromAnyRef("com.cloudera.labs.envelope.translate.DummyTranslator")));
    assertNoValidationFailures(new StreamingStep("ingest"), config);
  }

  @Test
  public void testTranslateWithRepartition() {
    int numPartitions = 10;
    Config stepConfig = ConfigFactory.empty()
        .withValue(StreamingStep.INPUT_TYPE, ConfigValueFactory.fromMap(new HashMap<String, Object>())
          .withValue("type", ConfigValueFactory.fromAnyRef("com.cloudera.labs.envelope.run.TestStreamingStep$PrepartitionedStreamInput"))          
          .withValue("repartition", ConfigValueFactory.fromMap(new HashMap<String, Object>())
            .withValue("partitions", ConfigValueFactory.fromAnyRef(Integer.toString(numPartitions))))
          .withValue("translator", ConfigValueFactory.fromMap(new HashMap<String, Object>())
            .withValue("type", ConfigValueFactory.fromAnyRef("com.cloudera.labs.envelope.translate.DummyTranslator")))
       );
    StreamingStep ss = new StreamingStep("TestRepartition");
    ss.configure(stepConfig);
    Config inputConfig = ConfigFactory.empty()
        .withValue("batch", ConfigValueFactory.fromMap(new HashMap<String, Object>())
          .withValue("size", ConfigValueFactory.fromAnyRef("1000"))
          .withValue("partitions", ConfigValueFactory.fromAnyRef(Integer.toString(2 * numPartitions)))
     );
    PrepartitionedStreamInput tsi = new PrepartitionedStreamInput() ;
    tsi.configure(inputConfig);
    JavaRDD<String> rdd = tsi.generateRDD();
    assertEquals( "Stream input was pre-partitioned with wrong number of partitions", 2 * numPartitions, rdd.getNumPartitions());

    Dataset<Row> translated = ss.translate(rdd);
    
    assertEquals( numPartitions, translated.rdd().getNumPartitions());
  }
  
  public static class PrepartitionedStreamInput implements StreamInput, Serializable {
    public static final long serialVersionUID = -1L;
    private long batchSize = 100L;
    private int partitions = 1;
    
    private StructType schema = DataTypes.createStructType(Lists.newArrayList(
      DataTypes.createStructField("value", DataTypes.StringType, true))
    );

    @Override
    public void configure(Config config) {
      if (config.hasPath("batch.size")) {
        batchSize = config.getLong("batch.size") ;
      }
      if (config.hasPath("batch.partitions")) {
        partitions = config.getInt("batch.partitions") ;
      }
    }

    @Override
    public StructType getProvidingSchema() {
      return schema;
    }

    @Override
    public JavaDStream<?> getDStream() throws Exception {
      Queue<JavaRDD<String>> queue = new LinkedList<>();
      queue.add(generateRDD());
      JavaDStream<String> dstream = Contexts.getJavaStreamingContext().queueStream(queue) ;
      return dstream;
    }
    
    public JavaRDD<String> generateRDD() {
      Random values = new Random();
      values.setSeed(System.currentTimeMillis());
      List<String> list = Lists.newLinkedList();
      for (int i = 0; i < batchSize; i++) {
        list.add(String.valueOf(values.nextLong()));
      }
      SparkContext sc = Contexts.getSparkSession().sparkContext();
      JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
      return jsc.parallelize(list,this.partitions);
    }

    @SuppressWarnings("serial")
    @Override
    public Function<?, Row> getMessageEncoderFunction() {
      return new Function<String, Row>() {
        @Override
        public Row call(String s) {
          return new RowWithSchema(schema, s);
        }
      };
    }
  }
  
}
