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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestAccumulatorRequest {
  
  @Test
  public void testWithoutInitialValue() {
    AccumulatorRequest requestInt = new AccumulatorRequest("hello", Integer.class);
    assertEquals(requestInt.getName(), "hello");
    assertEquals(requestInt.getClazz(), Integer.class);
    assertEquals(requestInt.getInitialValue(), 0);
    
    AccumulatorRequest requestDouble = new AccumulatorRequest("world", Double.class);
    assertEquals(requestDouble.getName(), "world");
    assertEquals(requestDouble.getClazz(), Double.class);
    assertEquals(requestDouble.getInitialValue(), 0.0);
  }
  
  @Test
  public void testWithInitialValue() {
    AccumulatorRequest requestInt = new AccumulatorRequest("hello", Integer.class, 10);
    assertEquals(requestInt.getName(), "hello");
    assertEquals(requestInt.getClazz(), Integer.class);
    assertEquals(requestInt.getInitialValue(), 10);
    
    AccumulatorRequest requestDouble = new AccumulatorRequest("world", Double.class, 100.0);
    assertEquals(requestDouble.getName(), "world");
    assertEquals(requestDouble.getClazz(), Double.class);
    assertEquals(requestDouble.getInitialValue(), 100.0);
  }
  
  @Test
  public void testEquality() {
    AccumulatorRequest request1;
    AccumulatorRequest request2;
    
    request1 = new AccumulatorRequest("hello", Integer.class, 10);
    request2 = new AccumulatorRequest("hello", Integer.class, 10);
    assertTrue(request1.equals(request2));
    
    request1 = new AccumulatorRequest("hello", Integer.class, 10);
    request2 = new AccumulatorRequest("hello", Integer.class, 20);
    assertTrue(request1.equals(request2));
    
    request1 = new AccumulatorRequest("hello", Integer.class, 10);
    request2 = new AccumulatorRequest("hello", Double.class, 10.0);
    assertTrue(request1.equals(request2));
    
    request1 = new AccumulatorRequest("hello", Integer.class, 10);
    request2 = new AccumulatorRequest("world", Integer.class, 10);
    assertFalse(request1.equals(request2));
  }
  
  @Test
  (expected = IllegalArgumentException.class)
  public void testUnsupportedClass() {
    new AccumulatorRequest("hello", Float.class, 10.0);
  }
  
  @Test
  (expected = IllegalArgumentException.class)
  public void testInvalidInitialValueInt() {
    new AccumulatorRequest("hello", Integer.class, 10.0);
  }
  
  @Test
  (expected = IllegalArgumentException.class)
  public void testInvalidInitialValueDouble() {
    new AccumulatorRequest("hello", Double.class, 10);
  }
  
}
