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
  private Comparable lower;
  private Comparable upper;
  private boolean ignoreNulls;
  private Class<? extends Comparable> fieldType = DEFAULT_FIELD_TYPE;

  @Override
  public void configure(String name, Config config) {
    this.name = name;
    fields = config.getStringList(FIELDS_CONFIG);
    if (config.hasPath(FIELD_TYPE_CONFIG)) {
      fieldType = getFieldType(config.getString(FIELD_TYPE_CONFIG));
    }
    List range = getValueList(fieldType, config.getAnyRefList(RANGE_CONFIG));
    lower = (Comparable) range.get(0);
    upper = (Comparable) range.get(1);
    ignoreNulls = ConfigUtils.getOrElse(config, IGNORE_NULLS_CONFIG, false);
  }

  @Override
  public boolean check(Row row) {
    for (String field : fields) {
      Comparable value;
      Object o = RowUtils.get(row, field);
      if (o != null) {
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
      else {
        return ignoreNulls;
      }
    }
    return true;
  }

  private boolean checkInternal(Comparable value, Comparable lower, Comparable upper) {
    return (value.compareTo(lower) >= 0 && value.compareTo(upper) <= 0);
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
