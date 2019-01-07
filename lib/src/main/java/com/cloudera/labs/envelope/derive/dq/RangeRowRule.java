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

package com.cloudera.labs.envelope.derive.dq;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validation;
import com.cloudera.labs.envelope.validate.ValidationResult;
import com.cloudera.labs.envelope.validate.Validations;
import com.cloudera.labs.envelope.validate.Validity;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Row;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RangeRowRule implements RowRule, ProvidesAlias, ProvidesValidations {

  public static final String RANGE_CONFIG = "range";
  public static final String FIELD_TYPE_CONFIG = "fieldtype";
  public static final String FIELDS_CONFIG = "fields";
  public static final String IGNORE_NULLS_CONFIG = "ignore-nulls";

  private static final Class<Long> DEFAULT_FIELD_TYPE = Long.class;

  private String name;
  private List<String> fields;
  private Comparable<?> lower;
  private Comparable<?> upper;
  private boolean ignoreNulls;
  private Class<? extends Comparable<?>> fieldType = DEFAULT_FIELD_TYPE;

  @Override
  public void configure(String name, Config config) {
    this.name = name;
    fields = config.getStringList(FIELDS_CONFIG);
    if (config.hasPath(FIELD_TYPE_CONFIG)) {
      fieldType = getFieldType(config.getString(FIELD_TYPE_CONFIG));
    }
    List<Number> range = getValueList(fieldType, config.getNumberList(RANGE_CONFIG));
    lower = (Comparable<?>) range.get(0);
    upper = (Comparable<?>) range.get(1);
    ignoreNulls = ConfigUtils.getOrElse(config, IGNORE_NULLS_CONFIG, false);
  }

  @Override
  public boolean check(Row row) {
    for (String field : fields) {
      Object value = RowUtils.get(row, field);
      if (value != null) {
        if (!(value instanceof Comparable)) {
          throw new RuntimeException("Range checkInternal on non-comparable type");
        }
        if (!checkInternal((Comparable<?>)value, lower, upper)) {
          return false;
        }
      }
      else {
        return ignoreNulls;
      }
    }
    return true;
  }

  private boolean checkInternal(Comparable value, Comparable lower, Comparable upper) {
    return (value.compareTo(lower) >= 0 && value.compareTo(upper) <= 0);
  }

  private static Class<? extends Comparable<?>> getFieldType(String fieldType) {
    Class<? extends Comparable<?>> clazz;
    switch (fieldType) {
      case "int":
        clazz = Integer.class;
        break;
      case "long":
        clazz = Long.class;
        break;
      case "double":
        clazz = Double.class;
        break;
      case "float":
        clazz = Float.class;
        break;
      case "decimal":
        clazz = BigDecimal.class;
        break;
      default:
        throw new RuntimeException("Cannot specify range of type [" + fieldType + "]");
    }

    return clazz;
  }

  private static List<Number> getValueList(Class<?> clazz, List<Number> values) {
    List<Number> valueList = new ArrayList<>();
    for (Number o : values) {
      if (clazz == Integer.class) {
        valueList.add(new Integer(o.intValue()));
      }
      else if (clazz == Long.class) {
        valueList.add(new Long(o.longValue()));
      }
      else if (clazz == Float.class) {
        valueList.add(new Float(o.floatValue()));
      }
      else if (clazz == Double.class) {
        valueList.add(new Double(o.doubleValue()));
      }
      else if (clazz == BigDecimal.class) {
        valueList.add(new BigDecimal(o.toString()));
      }
      else {
        throw new RuntimeException("Could not cast range values " + values + " to type [" + clazz + "]");
      }
    }
    return valueList;
  }

  @Override
  public String getAlias() {
    return "range";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(FIELDS_CONFIG, ConfigValueType.LIST)
        .mandatoryPath(RANGE_CONFIG, ConfigValueType.LIST)
        .optionalPath(FIELD_TYPE_CONFIG, ConfigValueType.STRING)
        .optionalPath(IGNORE_NULLS_CONFIG, ConfigValueType.BOOLEAN)
        .add(new Validation() {
          @Override
          public ValidationResult validate(Config config) {
            if (config.getAnyRefList(RANGE_CONFIG).size() != 2) {
              return new ValidationResult(this, Validity.INVALID, "Range must be a length-2 list");
            }
            else {
              return new ValidationResult(this, Validity.VALID, "Range is a length-2 list");
            }
          }
          @Override
          public Set<String> getKnownPaths() {
            return Sets.newHashSet(RANGE_CONFIG);
          }
        })
        .build();
  }
  
}
