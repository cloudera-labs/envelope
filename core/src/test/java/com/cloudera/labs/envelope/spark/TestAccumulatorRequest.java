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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestAccumulatorRequest {
  
  @Test
  public void testNewAccumulatorRequest() {
    AccumulatorRequest requestInt = new AccumulatorRequest("hello", Long.class);
    assertEquals(requestInt.getName(), "hello");
    assertEquals(requestInt.getClazz(), Long.class);
    
    AccumulatorRequest requestDouble = new AccumulatorRequest("world", Double.class);
    assertEquals(requestDouble.getName(), "world");
    assertEquals(requestDouble.getClazz(), Double.class);
  }
  
  @Test
  public void testEquality() {
    AccumulatorRequest request1;
    AccumulatorRequest request2;
    
    request1 = new AccumulatorRequest("hello", Long.class);
    request2 = new AccumulatorRequest("hello", Long.class);
    assertTrue(request1.equals(request2));
    
    request1 = new AccumulatorRequest("hello", Long.class);
    request2 = new AccumulatorRequest("hello", Long.class);
    assertTrue(request1.equals(request2));
    
    request1 = new AccumulatorRequest("hello", Long.class);
    request2 = new AccumulatorRequest("hello", Double.class);
    assertTrue(request1.equals(request2));
    
    request1 = new AccumulatorRequest("hello", Long.class);
    request2 = new AccumulatorRequest("world", Long.class);
    assertFalse(request1.equals(request2));
  }
  
  @Test
  (expected = IllegalArgumentException.class)
  public void testUnsupportedClass() {
    new AccumulatorRequest("hello", Float.class);
  }
  
}
