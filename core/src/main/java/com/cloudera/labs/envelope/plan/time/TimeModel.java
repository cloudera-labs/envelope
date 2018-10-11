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

package com.cloudera.labs.envelope.plan.time;

import com.cloudera.labs.envelope.component.Component;
import com.typesafe.config.Config;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.Comparator;
import java.util.List;

/**
 * A time model class contains a set of logic for representing time on a
 * row, for example a single long field that has milliseconds since epoch,
 * or a single string field that has a date with no time component.
 * 
 * A time model instance refers to specific field name(s) that the
 * instance will operate on, for example "lastupdated". A time model
 * instance does not belong to any particular Row object so that it
 * can be reused across rows.
 */
public interface TimeModel extends Component, Comparator<Row> {

  /**
   * Configure the time model instance.
   * @param config The configuration of the time model from the Envelope pipeline.
   * @param fieldNames The field names that this time model instance will apply to.
   */
  void configure(Config config, List<String> fieldNames);
  
  /**
   * Configure the current system time of the time model. This is separate
   * to the configure method because it could be called multiple times over
   * the lifetime of the instance.
   * @param currentSystemTimeMillis The current system time as milliseconds since epoch.
   */
  void configureCurrentSystemTime(long currentSystemTimeMillis);
  
  /**
   * Get the schema of the fields for the time model instance.
   */
  StructType getSchema();
  
  /**
   * Set the time model instance fields to a time in the far future.
   * At a minimum the far future should be after the year 2100, but
   * typically the date 9999-12-31 is used.
   * @param row The row to be set. This instance will not be modified.
   * @return The row with the far future time set.
   */
  Row setFarFutureTime(Row row);
  
  /**
   * Set the time model instance fields to the current system time.
   * @param row The row to be set. This instance will not be modified.
   * @return The row with the current system time set.
   */
  Row setCurrentSystemTime(Row row);
  
  /**
   * Set the time model instance fields to the preceding moment of the
   * current system time. The preceding moment is defined by the
   * granularity of time in the time model type.
   * @param row The row to be set. This instance will not be modified.
   * @return The row with the preceding system time set.
   */
  Row setPrecedingSystemTime(Row row);
  
  /**
   * Get a row that contains the time of the input row from the time
   * model instance fields.
   */
  Row getTime(Row row);
  
  /**
   * Get a row that contains the preceding time of the input row from
   * the time model instance fields. The preceding moment is defined by the
   * granularity of time in the time model type.
   */
  Row getPrecedingTime(Row row);
  
  /**
   * Append the time model instance fields to the row.
   * @param row The row to be appended. This instance will not be modified.
   * @return The row with the fields appended.
   */
  Row appendFields(Row row);
  
}
