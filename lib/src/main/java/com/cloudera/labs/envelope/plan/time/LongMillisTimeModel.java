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

import java.util.List;

public class LongMillisTimeModel implements TimeModel, ProvidesAlias {

  private StructField field;
  private Long current;

  @Override
  public void configure(Config config, List<String> fieldNames) {
    this.field = DataTypes.createStructField(fieldNames.get(0), DataTypes.LongType, true);
  }

  @Override
  public void configureCurrentSystemTime(long currentSystemTimeMillis) {
    this.current = currentSystemTimeMillis;
  }

  @Override
  public int compare(Row first, Row second) {
    Long ts1 = first.getAs(field.name());
    Long ts2 = second.getAs(field.name());
    
    if (ts1 < ts2) {
      return -1;
    } else if (ts1 > ts2) {
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
    return RowUtils.set(row, field.name(), current - 1);
  }

  @Override
  public StructType getSchema() {
    return DataTypes.createStructType(Lists.newArrayList(field));
  }

  @Override
  public Row setFarFutureTime(Row row) {
    return RowUtils.set(row, field.name(), 253402214400000L); // 31-DEC-9999 00:00:00.000
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
    return new RowWithSchema(getSchema(), row.<Long>getAs(field.name()) - 1);
  }

  @Override
  public String getAlias() {
    return "longmillis";
  }

}
