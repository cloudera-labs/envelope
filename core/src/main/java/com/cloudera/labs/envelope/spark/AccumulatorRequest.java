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

public class AccumulatorRequest {

  private String name;
  private Class<?> clazz;
  
  public AccumulatorRequest(String name, Class<?> clazz) {
    if (!clazz.equals(Long.class) && !clazz.equals(Double.class)) {
      throw new IllegalArgumentException("Accumulator user must request only long or double accumulators");
    }
    
    this.name = name;
    this.clazz = clazz;
  }
  
  public String getName() {
    return name;
  }

  public Class<?> getClazz() {
    return clazz;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null) return false;
    
    if (!(other instanceof AccumulatorRequest)) return false;
    
    // Accumulator requests are unique only by their name. If multiple objects request accumulators
    // with the same name but different classes then it is not defined which one Envelope will request from Spark.
    if (!((AccumulatorRequest)other).getName().equals(this.getName())) return false;
    
    return true;
  }

}
