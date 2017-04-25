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

public class AccumulatorRequest {

  private String name;
  private Class<?> clazz;
  private Object initialValue;
  
  public AccumulatorRequest(String name, Class<?> clazz) {
    this(name, clazz, clazz.equals(Integer.class) ? (Object)Integer.valueOf(0) : (Object)Double.valueOf(0.0));
  }
  
  public AccumulatorRequest(String name, Class<?> clazz, Object initialValue) {
    if (!clazz.equals(Integer.class) && !clazz.equals(Double.class)) {
      throw new IllegalArgumentException("Accumulator user must request only integer or double accumulators");
    }
    
    if (!initialValue.getClass().equals(clazz)) {
      throw new IllegalArgumentException("Accumulator initial value must match the requested accumulator data type");
    }
    
    this.name = name;
    this.clazz = clazz;
    this.initialValue = initialValue;
  }
  
  public String getName() {
    return name;
  }

  public Class<?> getClazz() {
    return clazz;
  }

  public Object getInitialValue() {
    return initialValue;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null) return false;
    
    if (!(other instanceof AccumulatorRequest)) return false;
    
    // Accumulator requests are unique only by their name. If multiple objects request accumulators
    // with the same name but different classes or initial values then it is not defined which one
    // Envelope will request from Spark.
    if (!((AccumulatorRequest)other).getName().equals(this.getName())) return false;
    
    return true;
  }

}
