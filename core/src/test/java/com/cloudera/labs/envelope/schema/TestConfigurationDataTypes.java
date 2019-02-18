/*
 * Copyright (c) 2015-2019, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.labs.envelope.schema;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestConfigurationDataTypes {

  @Test
  public void testGetConfigurationDataTypeValid() {
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(new DecimalType()), "decimal(10,0)");
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(new DecimalType(38,38)), "decimal(38,38)");
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(DataTypes.StringType), ConfigurationDataTypes.STRING);
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(DataTypes.FloatType), ConfigurationDataTypes.FLOAT);
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(DataTypes.DoubleType), ConfigurationDataTypes.DOUBLE);
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(DataTypes.ByteType), ConfigurationDataTypes.BYTE);
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(DataTypes.ShortType), ConfigurationDataTypes.SHORT);
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(DataTypes.IntegerType), ConfigurationDataTypes.INTEGER);
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(DataTypes.LongType), ConfigurationDataTypes.LONG);
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(DataTypes.BooleanType), ConfigurationDataTypes.BOOLEAN);
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(DataTypes.BinaryType), ConfigurationDataTypes.BINARY);
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(DataTypes.DateType), ConfigurationDataTypes.DATE);
    assertEquals(ConfigurationDataTypes.getConfigurationDataType(DataTypes.TimestampType), ConfigurationDataTypes.TIMESTAMP);
  }

  @Test (expected = RuntimeException.class) 
  public void testGetConfigurationDataTypeInvalid() {
    ConfigurationDataTypes.getConfigurationDataType(new ArrayType());
  }

  @Test
  public void testGetSparkDataTypeValid() {
    assertEquals(ConfigurationDataTypes.getSparkDataType(ConfigurationDataTypes.DECIMAL), new DecimalType());
    assertEquals(ConfigurationDataTypes.getSparkDataType("decimal(38,38)"), new DecimalType(38,38));
    assertEquals(ConfigurationDataTypes.getSparkDataType("decimal ( 38 , 38 ) "), new DecimalType(38,38));
    assertEquals(ConfigurationDataTypes.getSparkDataType(ConfigurationDataTypes.STRING), DataTypes.StringType);
    assertEquals(ConfigurationDataTypes.getSparkDataType(ConfigurationDataTypes.FLOAT), DataTypes.FloatType);
    assertEquals(ConfigurationDataTypes.getSparkDataType(ConfigurationDataTypes.DOUBLE), DataTypes.DoubleType);
    assertEquals(ConfigurationDataTypes.getSparkDataType(ConfigurationDataTypes.BYTE), DataTypes.ByteType);
    assertEquals(ConfigurationDataTypes.getSparkDataType(ConfigurationDataTypes.SHORT), DataTypes.ShortType);
    assertEquals(ConfigurationDataTypes.getSparkDataType(ConfigurationDataTypes.INTEGER), DataTypes.IntegerType);
    assertEquals(ConfigurationDataTypes.getSparkDataType(ConfigurationDataTypes.LONG), DataTypes.LongType);
    assertEquals(ConfigurationDataTypes.getSparkDataType(ConfigurationDataTypes.BOOLEAN), DataTypes.BooleanType);
    assertEquals(ConfigurationDataTypes.getSparkDataType(ConfigurationDataTypes.BINARY), DataTypes.BinaryType);
    assertEquals(ConfigurationDataTypes.getSparkDataType(ConfigurationDataTypes.DATE), DataTypes.DateType);
    assertEquals(ConfigurationDataTypes.getSparkDataType(ConfigurationDataTypes.TIMESTAMP), DataTypes.TimestampType);
  }

  @Test (expected = RuntimeException.class)
  public void testGetSparkDataTypeInvalid() {
    ConfigurationDataTypes.getSparkDataType("array");
  }
}
