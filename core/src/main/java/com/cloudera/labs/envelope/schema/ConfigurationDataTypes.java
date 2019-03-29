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

import com.google.common.collect.Sets;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigurationDataTypes {

  public static final String DECIMAL = "decimal";
  public static final String STRING = "string";
  public static final String FLOAT = "float";
  public static final String DOUBLE = "double";
  public static final String BYTE = "byte";
  public static final String SHORT = "short";
  public static final String INT = "int";
  public static final String LONG = "long";
  public static final String BOOLEAN = "boolean";
  public static final String BINARY = "binary";
  public static final String DATE = "date";
  public static final String TIMESTAMP = "timestamp";

  public static String getConfigurationDataType(DataType type) {
    Set<String> supportedTypes = Sets.newHashSet(
        STRING, FLOAT, DOUBLE, BYTE, SHORT, INT,
        LONG, BOOLEAN, BINARY, DATE, TIMESTAMP);
 
    if (supportedTypes.contains(type.typeName()) || (type instanceof DecimalType)) {
      return type.typeName();
    }
    else if (type.typeName().equals("integer")) {
      return INT;
    }
    else {
      throw new RuntimeException("Unsupported field type: " + type);
    }

  }

  public static DataType getSparkDataType(String typeString) {
    DataType type;

    String prec_scale_regex_groups = "\\s*(decimal)\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)\\s*";
    Pattern prec_scale_regex_pattern = Pattern.compile(prec_scale_regex_groups);
    Matcher prec_scale_regex_matcher = prec_scale_regex_pattern.matcher(typeString);

    if (prec_scale_regex_matcher.matches()) {
      int precision = Integer.parseInt(prec_scale_regex_matcher.group(2)); 
      int scale = Integer.parseInt(prec_scale_regex_matcher.group(3)); 
      type = DataTypes.createDecimalType(precision, scale);
    }
    else {
      switch (typeString) {
        case DECIMAL:
          type = DataTypes.createDecimalType();
          break;
        case STRING:
          type = DataTypes.StringType;
          break;
        case FLOAT:
          type = DataTypes.FloatType;
          break;
        case DOUBLE:
          type = DataTypes.DoubleType;
          break;
        case BYTE:
          type = DataTypes.ByteType;
          break;
        case SHORT:
          type = DataTypes.ShortType;
          break;
        case INT:
          type = DataTypes.IntegerType;
          break;
        case LONG:
          type = DataTypes.LongType;
          break;
        case BOOLEAN:
          type = DataTypes.BooleanType;
          break;
        case BINARY:
          type = DataTypes.BinaryType;
          break;
        case DATE:
          type = DataTypes.DateType;
          break;
        case TIMESTAMP:
          type = DataTypes.TimestampType;
          break;
        default:
          throw new RuntimeException("Unsupported or unrecognized field type: " + typeString);
      } 
    }

    return type;
  }

}
