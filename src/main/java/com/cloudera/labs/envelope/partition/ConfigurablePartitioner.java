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
package com.cloudera.labs.envelope.partition;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;

import com.typesafe.config.Config;

/**
 * Partitioners determine the logic for how Envelope should group keys together into RDD partitions
 * for requesting existing records from an output.
 */
@SuppressWarnings("serial")
public abstract class ConfigurablePartitioner extends Partitioner {

  /**
   * Configure the partitioner.
   * @param config The configuration of the partitioner.
   * @param rdd The RDD of keyed arriving records. This may be used to determine the suggest the
   * number of partitions, or to read arriving records for the partitioning logic.
   */
  public abstract void configure(Config config, JavaPairRDD<Row, Row> rdd);

}
