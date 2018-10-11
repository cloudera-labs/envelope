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

package com.cloudera.labs.envelope.spark;

import com.google.common.collect.Maps;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("serial")
public class Accumulators implements Serializable {
  
  private Map<String, LongAccumulator> longAccumulators = Maps.newHashMap();
  private Map<String, DoubleAccumulator> doubleAccumulators = Maps.newHashMap();
  
  private static Logger LOG = LoggerFactory.getLogger(Accumulators.class);
  
  public Accumulators(Set<AccumulatorRequest> requests) {
    for (AccumulatorRequest request : requests) {
      String name = request.getName();
      Class<?> clazz = request.getClazz();
      
      if (clazz == Long.class) {
        LongAccumulator acc = Contexts.getSparkSession().sparkContext().longAccumulator(name);
        longAccumulators.put(name, acc);
      }
      
      if (clazz == Double.class) {
        DoubleAccumulator acc = Contexts.getSparkSession().sparkContext().doubleAccumulator(name);
        doubleAccumulators.put(name, acc);
      }
      
      LOG.info("Processed accumulator request: " + name);
    }
  }
  
  public Map<String, LongAccumulator> getLongAccumulators() {
    return longAccumulators;
  }
  
  public Map<String, DoubleAccumulator> getDoubleAccumulators() {
    return doubleAccumulators;
  }
  
}
