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
package com.cloudera.labs.envelope.spark;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.apache.spark.Accumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

@SuppressWarnings("serial")
public class Accumulators implements Serializable {
  
  private Map<String, Accumulator<Integer>> intAccumulators = Maps.newHashMap();
  private Map<String, Accumulator<Double>> doubleAccumulators = Maps.newHashMap();
  
  private static Logger LOG = LoggerFactory.getLogger(Accumulators.class);
  
  public Accumulators(Set<AccumulatorRequest> requests) {
    for (AccumulatorRequest request : requests) {
      String name = request.getName();
      Class<?> clazz = request.getClazz();
      Object initialValue = request.getInitialValue();
      
      if (clazz == Integer.class) {
        Accumulator<Integer> acc = Contexts.getJavaSparkContext().intAccumulator((int)initialValue, name);
        intAccumulators.put(name, acc);
      }
      
      if (clazz == Double.class) {
        Accumulator<Double> acc = Contexts.getJavaSparkContext().doubleAccumulator((double)initialValue, name);
        doubleAccumulators.put(name, acc);
      }
      
      LOG.info("Processed accumulator request: " + name);
    }
  }
  
  public Map<String, Accumulator<Integer>> getIntAccumulators() {
    return intAccumulators;
  }
  
  public Map<String, Accumulator<Double>> getDoubleAccumulators() {
    return doubleAccumulators;
  }
  
}
