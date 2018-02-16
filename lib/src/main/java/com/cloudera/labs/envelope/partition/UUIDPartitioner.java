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
package com.cloudera.labs.envelope.partition;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.typesafe.config.Config;

@SuppressWarnings("serial")
public class UUIDPartitioner extends ConfigurablePartitioner implements ProvidesAlias {

  int numPartitions;
  
  @Override
  public void configure(Config config, JavaPairRDD<Row, Row> rdd) {
    this.numPartitions = rdd.getNumPartitions();
  }
  
  @Override
  public int getPartition(Object rowObject) {
    Row row = (Row)rowObject;
    int n = 4;
    
    int firstN = Integer.parseInt(row.getString(0).substring(0, n).toUpperCase(), 16);
    
    return (int)(firstN / Math.pow(16, n) * numPartitions());
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }

  @Override
  public String getAlias() {
    return "uuid";
  }
}
