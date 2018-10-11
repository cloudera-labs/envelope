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

import java.math.BigDecimal;
import java.util.List;

public class NanosWithSeqNumTimeModel implements TimeModel, ProvidesAlias {

  private StructField nanoField;
  private StructField seqNumField;
  private BigDecimal current;
  
  // 31-DEC-9999 00:00:00.000000000
  private static final BigDecimal farFuture = new BigDecimal("253402214400000000000");
  private static final int firstSeqNum = 1;


  @Override
  public void configure(Config config, List<String> fieldNames) {
    this.nanoField = DataTypes.createStructField(fieldNames.get(0), DataTypes.createDecimalType(38, 0), true);
    this.seqNumField = DataTypes.createStructField(fieldNames.get(1), DataTypes.IntegerType, true);    
  }

  @Override
  public void configureCurrentSystemTime(long currentSystemTimeMillis) {
    this.current = new BigDecimal(currentSystemTimeMillis).multiply(new BigDecimal(1000 * 1000));
  }
  
  @Override
  public int compare(Row first, Row second) {
    BigDecimal firstNanos = first.<BigDecimal>getAs(nanoField.name());
    BigDecimal secondNanos = second.<BigDecimal>getAs(nanoField.name());
    Integer firstSeqNum = first.<Integer>getAs(seqNumField.name());
    Integer secondSeqNum = second.<Integer>getAs(seqNumField.name());
    
    if (firstNanos.equals(secondNanos)) {
      return firstSeqNum.compareTo(secondSeqNum);
    }
    else {
      return firstNanos.compareTo(secondNanos);
    }
  }

  @Override
  public StructType getSchema() {
    return DataTypes.createStructType(Lists.newArrayList(nanoField, seqNumField));
  }

  @Override
  public Row setFarFutureTime(Row row) {
    row = RowUtils.set(row, nanoField.name(), farFuture);
    row = RowUtils.set(row, seqNumField.name(), firstSeqNum);
    
    return row;
  }

  @Override
  public Row setCurrentSystemTime(Row row) {
    row = RowUtils.set(row, nanoField.name(), current);
    row = RowUtils.set(row, seqNumField.name(), firstSeqNum);
    
    return row;
  }

  @Override
  public Row setPrecedingSystemTime(Row row) {
    row = RowUtils.set(row, nanoField.name(), current.subtract(BigDecimal.ONE));
    row = RowUtils.set(row, seqNumField.name(), Integer.MAX_VALUE);
    
    return row;
  }

  @Override
  public Row appendFields(Row row) {
    row = RowUtils.append(row, nanoField.name(), nanoField.dataType(), null);
    row = RowUtils.append(row, seqNumField.name(), seqNumField.dataType(), null);
    
    return row;
  }

  @Override
  public Row getTime(Row row) {
    return new RowWithSchema(getSchema(), 
        RowUtils.get(row, nanoField.name()), RowUtils.get(row, seqNumField.name()));
  }

  @Override
  public Row getPrecedingTime(Row row) {
    BigDecimal timeNanos = row.<BigDecimal>getAs(nanoField.name());
    int timeSeqNum = row.<Integer>getAs(seqNumField.name());
    
    if (timeSeqNum > firstSeqNum) {
      timeSeqNum = timeSeqNum - 1;
    }
    else {
      timeNanos = timeNanos.subtract(BigDecimal.ONE);
      timeSeqNum = Integer.MAX_VALUE;
    }
    
    return new RowWithSchema(getSchema(), timeNanos, timeSeqNum);
  }
  
  @Override
  public String getAlias() {
    return "nanoswithseqnum";
  }

}
