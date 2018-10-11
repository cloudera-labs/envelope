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

package com.cloudera.labs.envelope.partition;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;

@SuppressWarnings("serial")
public class DummyPartitioner extends ConfigurablePartitioner {
  boolean configured = false;
      
  @Override
  public void configure(Config config, JavaPairRDD<Row, Row> rdd) {
    configured = true;
  }

  @Override
  public int getPartition(Object arg0) {
    return 0;
  }

  @Override
  public int numPartitions() {
    return 0;
  }
  
  public boolean hasBeenConfigured() {
    return configured;
  }
}