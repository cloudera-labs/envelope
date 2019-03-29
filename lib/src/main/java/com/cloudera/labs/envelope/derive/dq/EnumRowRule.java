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

package com.cloudera.labs.envelope.derive.dq;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.schema.ConfigurationDataTypes;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Row;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EnumRowRule implements RowRule, ProvidesAlias, ProvidesValidations {

  private static final String FIELDS_CONFIG = "fields";
  private static final String FIELD_TYPE_CONFIG = "fieldtype";
  private static final String VALUES_CONFIG = "values";
  private static final String CASE_SENSITIVE_CONFIG = "case-sensitive";

  private static final boolean DEFAULT_CASE_SENSITIVITY = true;

  private Set validValues;
  private List<String> fields;
  private Class fieldType;
  private boolean caseSensitive;

  @Override
  public void configure(Config config) {
    this.caseSensitive = ConfigUtils.getOrElse(config, CASE_SENSITIVE_CONFIG, DEFAULT_CASE_SENSITIVITY);
    this.fieldType = getFieldType(config.getString(FIELD_TYPE_CONFIG));
    this.validValues = getValueSet(fieldType, config.getAnyRefList(VALUES_CONFIG));
    if (!caseSensitive && fieldType == String.class) {
      Set<String> replacementValues = new HashSet<>();
      for (Object o : validValues) {
        replacementValues.add(((String)o).toLowerCase());
      }
      validValues = replacementValues;
    }
    this.fields = config.getStringList(FIELDS_CONFIG);
  }

  @Override
  public void configureName(String name) {
    // Not used
  }

  @Override
  public boolean check(Row row) {
    boolean check = true;
    for (String field : fields) {
      if (fieldType == String.class && !caseSensitive) {
        check = check && validValues.contains(row.<String>getAs(field).toLowerCase());
      } else {
        check = check && validValues.contains(row.getAs(field));
      }
    }
    return check;
  }

  private static Class getFieldType(String fieldType) {
    Class clazz;
    switch (fieldType) {
      case ConfigurationDataTypes.INT:
        clazz = Integer.class;
        break;
      case ConfigurationDataTypes.LONG:
        clazz = Long.class;
        break;
      case ConfigurationDataTypes.BOOLEAN:
        clazz = Boolean.class;
        break;
      case ConfigurationDataTypes.DOUBLE:
      case ConfigurationDataTypes.FLOAT:
        throw new RuntimeException("Cannot specify inexact floating point types in EnumRowRule");
      case ConfigurationDataTypes.DECIMAL:
        clazz = BigDecimal.class;
        break;
      default:
        clazz = String.class;
        break;
    }

    return clazz;
  }

  private static <T> Set<T> getValueSet(Class<T> clazz, List values) {
    Set<T> valueSet = new HashSet<>();
    for (Object o : values) {
      if (clazz == BigDecimal.class) {
        o = new BigDecimal((String)o);
      }
      if (!valueSet.add(clazz.cast(o))) {
        throw new RuntimeException("Could not cast object to type [" + clazz + "]");
      }
    }
    return valueSet;
  }

  @Override
  public String getAlias() {
    return "enum";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(FIELDS_CONFIG, ConfigValueType.LIST)
        .mandatoryPath(VALUES_CONFIG, ConfigValueType.LIST)
        .mandatoryPath(FIELD_TYPE_CONFIG, ConfigValueType.STRING)
        .optionalPath(CASE_SENSITIVE_CONFIG, ConfigValueType.BOOLEAN)
        .build();
  }
  
}
