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

package com.cloudera.labs.envelope.component;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *  This interface should be implemented when a component wants to capture errored data.
 * 
 *  Envelope uses this interface to retrieve errored data and register it as a datastep that
 *  can be referenced for further processing. 
 * 
 *  This is currently only supported for inputs and derivers
 */
public interface CanReturnErroredData {
    
  /**
   * Returns the data that errored out
   */  
  Dataset<Row> getErroredData();

}
