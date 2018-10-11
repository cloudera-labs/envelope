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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Allows individual Spark SQL Rows to be created with a schema. This is achieved by
 * simply wrapping a RowFactory-created Row and overriding the schema methods.
 */
@SuppressWarnings("serial")
public class RowWithSchema implements Row {

  StructType schema;
  Row internalRow;

  public RowWithSchema(StructType schema, Object... values) {
    this.schema = schema;
    this.internalRow = RowFactory.create(values);
  }

  @Override
  public boolean anyNull() {
    return internalRow.anyNull();
  }

  @Override
  public Object apply(int arg0) {
    return internalRow.apply(arg0);
  }

  @Override
  public Row copy() {
    return internalRow.copy();
  }

  @Override
  public int fieldIndex(String arg0) {
    return schema.fieldIndex(arg0);
  }

  @Override
  public Object get(int arg0) {
    return internalRow.get(arg0);
  }

  @Override
  public <T> T getAs(String arg0) {
    return internalRow.getAs(fieldIndex(arg0));
  }

  @Override
  public boolean getBoolean(int arg0) {
    return internalRow.getBoolean(arg0);
  }

  @Override
  public byte getByte(int arg0) {
    return internalRow.getByte(arg0);
  }

  @Override
  public Date getDate(int arg0) {
    return internalRow.getDate(arg0);
  }

  @Override
  public BigDecimal getDecimal(int arg0) {
    return internalRow.getDecimal(arg0);
  }

  @Override
  public double getDouble(int arg0) {
    return internalRow.getDouble(arg0);
  }

  @Override
  public float getFloat(int arg0) {
    return internalRow.getFloat(arg0);
  }

  @Override
  public int getInt(int arg0) {
    return internalRow.getInt(arg0);
  }

  @Override
  public <K, V> Map<K, V> getJavaMap(int arg0) {
    return internalRow.getJavaMap(arg0);
  }

  @Override
  public <T> List<T> getList(int arg0) {
    return internalRow.getList(arg0);
  }

  @Override
  public long getLong(int arg0) {
    return internalRow.getLong(arg0);
  }

  @Override
  public <K, V> scala.collection.Map<K, V> getMap(int arg0) {
    return internalRow.getMap(arg0);
  }

  @Override
  public <T> Seq<T> getSeq(int arg0) {
    return internalRow.getSeq(arg0);
  }

  @Override
  public short getShort(int arg0) {
    return internalRow.getShort(arg0);
  }

  @Override
  public String getString(int arg0) {
    return internalRow.getString(arg0);
  }

  @Override
  public Row getStruct(int arg0) {
    return internalRow.getStruct(arg0);
  }

  @Override
  public Timestamp getTimestamp(int arg0) {
    return internalRow.getTimestamp(arg0);
  }

  @Override
  public <T> scala.collection.immutable.Map<String, T> getValuesMap(Seq<String> arg0) {
    return internalRow.getValuesMap(arg0);
  }

  @Override
  public boolean isNullAt(int arg0) {
    return internalRow.isNullAt(arg0);
  }

  @Override
  public int length() {
    return internalRow.length();
  }

  @Override
  public String mkString() {
    return internalRow.mkString();
  }

  @Override
  public String mkString(String arg0) {
    return internalRow.mkString(arg0);
  }

  @Override
  public String mkString(String arg0, String arg1, String arg2) {
    return internalRow.mkString(arg0, arg1, arg2);
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public int size() {
    return internalRow.size();
  }

  @Override
  public Seq<Object> toSeq() {
    return internalRow.toSeq();
  }

  @Override
  public <T> T getAs(int arg0) {
    return internalRow.getAs(arg0);
  }

  @Override
  public String toString() {
    return internalRow.toString();
  }

  @Override
  public boolean equals(Object other) {
    return internalRow.equals(other);
  }

  @Override
  public int hashCode() {
    return internalRow.hashCode();
  }

}
