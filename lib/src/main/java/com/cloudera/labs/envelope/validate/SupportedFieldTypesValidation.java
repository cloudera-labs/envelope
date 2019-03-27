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

package com.cloudera.labs.envelope.validate;

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.schema.Schema;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;

import java.util.HashSet;
import java.util.Set;

public class SupportedFieldTypesValidation implements Validation {
  private String path;
  
  private Set<DataType> validationTypes = new HashSet<DataType>();

  public SupportedFieldTypesValidation(String path, Set<DataType> validTypes) {
    this.path = path;

    for (DataType type : validTypes) {
      if (type instanceof DecimalType) {
        this.validationTypes.add(new DecimalType());
      }
      else {
        this.validationTypes.add(type);
      }
    }
  }

  @Override
  public ValidationResult validate(Config config) {

    for (StructField field : ComponentFactory.create(
        Schema.class, config.getConfig(this.path), true).getSchema().fields()) {
      boolean decimalMatch = (field.dataType() instanceof DecimalType &&
                              validationTypes.contains(new DecimalType()));

      if (!validationTypes.contains(field.dataType()) && !decimalMatch) {
        return new ValidationResult(this, Validity.INVALID,
          "Schema field type " + field.dataType().simpleString() + " is not supported by this component type.");
      }
    }
    return new ValidationResult(this, Validity.VALID, "Schema field types are valid for this component type.");
  }

  @Override
  public Set<String> getKnownPaths() {
    return Sets.newHashSet(path);
  }
}
