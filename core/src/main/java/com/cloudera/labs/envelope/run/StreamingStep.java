/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.run;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.cloudera.labs.envelope.input.CanRecordProgress;
import com.cloudera.labs.envelope.input.StreamInput;
import com.cloudera.labs.envelope.input.translate.TranslateFunction;
import com.typesafe.config.Config;

/**
 * A streaming step is a data step that provides a DataFrame per Spark Streaming micro-batch.
 */
public class StreamingStep extends DataStep implements CanRecordProgress {

  public static final String REPARTITION_NUM_PARTITIONS_PROPERTY = "input.repartition.partitions";
  
  @SuppressWarnings("rawtypes")
  private TranslateFunction translateFunction;

  public StreamingStep(String name, Config config) {
    super(name, config);
    
    if (!config.hasPath("input.translator")) {
      throw new RuntimeException("Stream input '" + name + "' must have a translator");
    }
    
    translateFunction = new TranslateFunction<>(config.getConfig("input.translator"));
  }

  @SuppressWarnings("rawtypes")
  public JavaDStream<?> getStream() throws Exception {
    JavaDStream stream = ((StreamInput)getInput()).getDStream();
    
    if (doesRepartition()) {
      stream = repartition(stream);
    }

    return stream;
  }
  
  public StructType getSchema() {
    return translateFunction.getSchema();
  }
  
  @Override
  public void stageProgress(JavaRDD<?> batch) {
    if (((StreamInput)getInput()) instanceof CanRecordProgress) {
      ((CanRecordProgress)getInput()).stageProgress(batch);
    }
  }
  
  @Override
  public void recordProgress() throws Exception {
    if (((StreamInput)getInput()) instanceof CanRecordProgress) {
      ((CanRecordProgress)getInput()).recordProgress();
    }
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public JavaRDD<Row> translate(JavaRDD raw) {
    JavaPairRDD<?, ?> prepared = raw.mapToPair(((StreamInput)getInput()).getPrepareFunction());
    JavaRDD<Row> translated = prepared.flatMap(translateFunction);
    
    return translated;
  }

  @Override
  public Step copy() {
    StreamingStep copy = new StreamingStep(name, config);
    
    copy.setSubmitted(hasSubmitted());
    
    if (hasSubmitted()) {
      copy.setData(getData());
    }
    
    return copy;
  }

  private boolean doesRepartition() {
    return config.hasPath(REPARTITION_NUM_PARTITIONS_PROPERTY);
  }

  private JavaDStream<?> repartition(JavaDStream<?> stream) {
    int numPartitions = config.getInt(REPARTITION_NUM_PARTITIONS_PROPERTY);

    return stream.repartition(numPartitions);
  }

}
