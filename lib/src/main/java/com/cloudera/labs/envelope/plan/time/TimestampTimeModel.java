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

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.List;

public class TimestampTimeModel implements TimeModel, ProvidesAlias {

  private StructField field;
  private Timestamp current;
  private Timestamp farFuture = new Timestamp(253402214400000L);

  @Override
  public void configure(Config config, List<String> fieldNames) {
    this.field = DataTypes.createStructField(fieldNames.get(0), DataTypes.TimestampType, true);
  }

  @Override
  public void configureCurrentSystemTime(long currentSystemTimeMillis) {
    this.current = new Timestamp(currentSystemTimeMillis);
  }

  @Override
  public int compare(Row first, Row second) {
    Timestamp ts1 = first.getAs(field.name());
    Timestamp ts2 = second.getAs(field.name());
    
    if (ts1.before(ts2)) {
      return -1;
    } else if (ts1.after(ts2)) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public Row setCurrentSystemTime(Row row) {
    return RowUtils.set(row, field.name(), current);
  }

  @Override
  public Row setPrecedingSystemTime(Row row) {
    return RowUtils.set(row, field.name(), getPrecedingTimestamp(current));
  }

  @Override
  public StructType getSchema() {
    return DataTypes.createStructType(Lists.newArrayList(field));
  }

  @Override
  public Row setFarFutureTime(Row row) {
    // 31-DEC-9999 00:00:00.000000000
    return RowUtils.set(row, field.name(), farFuture);
  }

  @Override
  public Row appendFields(Row row) {
    return RowUtils.append(row, field.name(), field.dataType(), null);
  }

  @Override
  public Row getTime(Row row) {
    return new RowWithSchema(getSchema(), RowUtils.get(row, field.name()));
  }

  @Override
  public Row getPrecedingTime(Row row) {
    Timestamp time = row.<Timestamp>getAs(field.name());
    
    return new RowWithSchema(getSchema(), getPrecedingTimestamp(time));
  }
  
  private Timestamp getPrecedingTimestamp(Timestamp time) {
    long timeMillis = time.getTime() - (time.getTime() % 1000);
    int timeNanos = time.getNanos();
    
    if (timeNanos > 0) {
      timeNanos = timeNanos - 1;
    }
    else {
      timeMillis = timeMillis - 1000;
      timeNanos = 999999999;
    }
    
    Timestamp preceding = new Timestamp(timeMillis);
    preceding.setNanos(timeNanos);
    
    return preceding;
  }
  
  @Override
  public String getAlias() {
    return "timestamp";
  }

}
