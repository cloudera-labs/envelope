/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.derive.dq;

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.Row;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class RangeRowRule implements RowRule {

  private static final String RANGE_CONFIG = "range";
  private static final String FIELD_TYPE_CONFIG = "fieldtype";
  private static final String FIELDS_CONFIG = "fields";

  private static final Class<Long> DEFAULT_FIELD_TYPE = Long.class;

  private String name;
  private List<String> fields;
  private Comparable lower;
  private Comparable upper;
  private Class<? extends Comparable> fieldType = DEFAULT_FIELD_TYPE;

  @Override
  public void configure(String name, Config config) {
    this.name = name;
    ConfigUtils.assertConfig(config, FIELDS_CONFIG);
    fields = config.getStringList(FIELDS_CONFIG);
    if (config.hasPath(FIELD_TYPE_CONFIG)) {
      fieldType = getFieldType(config.getString(FIELD_TYPE_CONFIG));
    }
    ConfigUtils.assertConfig(config, RANGE_CONFIG);
    List range = getValueList(fieldType, config.getAnyRefList(RANGE_CONFIG));
    if (range.size() == 2) {
      lower = (Comparable) range.get(0);
      upper = (Comparable) range.get(1);
    } else {
      throw new RuntimeException("Range must be a length-2 list");
    }
  }

  @Override
  public boolean check(Row row) {
    for (String field : fields) {
      Comparable value;
      Object o = RowUtils.get(row, field);
      if (o instanceof Number) {
        if (o instanceof Float) {
          value = ((Float)o).doubleValue();
        } else {
          value = (Comparable)o;
        }
      } else {
        throw new RuntimeException("Range checkInternal on non-numeric type");
      }

      if (!checkInternal(value, lower, upper)) {
        return false;
      }
    }
    return true;
  }

  private boolean checkInternal(Comparable value, Comparable lower, Comparable upper) {
    if (value != null) {
      return (value.compareTo(lower) >= 0 && value.compareTo(upper) <= 0);
    }
    else throw new RuntimeException("Attempt to compare null");
  }

  private static Class<? extends Comparable> getFieldType(String fieldType) {
    Class<? extends Comparable> clazz;
    switch (fieldType) {
      case "int":
        clazz = Integer.class;
        break;
      case "long":
        clazz = Long.class;
        break;
      case "double":
      case "float":
        // Typesafe config does not parse floats
        clazz = Double.class;
        break;
      case "decimal":
        clazz = BigDecimal.class;
        break;
      default:
        throw new RuntimeException("Cannot specify range of type [" + fieldType + "]");
    }

    return clazz;
  }

  private static <T> List<T> getValueList(Class<T> clazz, List values) {
    List<T> valueList = new ArrayList<>();
    for (Object o : values) {
      if (clazz == BigDecimal.class) {
        o = new BigDecimal((String)o);
      }
      if (!valueList.add(clazz.cast(o))) {
        throw new RuntimeException("Could not cast object to type [" + clazz + "]");
      }
    }
    return valueList;
  }

  @Override
  public String getAlias() {
    return "range";
  }
}
