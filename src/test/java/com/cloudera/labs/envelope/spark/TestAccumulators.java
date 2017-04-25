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

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.apache.spark.Accumulator;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestAccumulators {

  @Test
  public void testRequestOne() {
    AccumulatorRequest request = new AccumulatorRequest("hello", Integer.class, 20);
    
    Accumulators accumulators = new Accumulators(Collections.singleton(request));
    
    Accumulator<Integer> accumulator = accumulators.getIntAccumulators().get("hello");
    assertEquals(accumulator.name().get(), "hello");
    assertEquals(accumulator.initialValue(), (Integer)20);
  }
  
  @Test
  public void testRequestMany() {
    AccumulatorRequest request1 = new AccumulatorRequest("hello", Integer.class, 20);
    AccumulatorRequest request2 = new AccumulatorRequest("world", Double.class, 200.0);
    
    Accumulators accumulators = new Accumulators(Sets.newHashSet(request1, request2));
    
    Accumulator<Integer> accumulator1 = accumulators.getIntAccumulators().get("hello");
    assertEquals(accumulator1.name().get(), "hello");
    assertEquals(accumulator1.initialValue(), (Integer)20);
    
    Accumulator<Double> accumulator2 = accumulators.getDoubleAccumulators().get("world");
    assertEquals(accumulator2.name().get(), "world");
    assertEquals(accumulator2.initialValue(), (Double)200.0);
  }
  
  @After
  public void after() {
    Contexts.getJavaSparkContext().close();
  }
  
}
