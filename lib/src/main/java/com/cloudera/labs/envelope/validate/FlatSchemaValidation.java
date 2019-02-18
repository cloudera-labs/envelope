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

import com.cloudera.labs.envelope.schema.ConfigurationDataTypes;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import java.util.List;
import java.util.Set;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FlatSchemaValidation implements Validation {

  private String namesPath;
  private String typesPath;

  public FlatSchemaValidation(String names, String types) {
    this.namesPath = names;
    this.typesPath = types;
  }

  @Override
  public ValidationResult validate(Config config) {
    try {
      List<String> names = config.getStringList(this.namesPath);
      List<String> types = config.getStringList(this.typesPath);
      List<StructField> fields = Lists.newArrayList();

      for (int fieldNum = 0; fieldNum < names.size(); fieldNum++) {
        fields.add(DataTypes.createStructField(names.get(fieldNum),
          ConfigurationDataTypes.getSparkDataType(types.get(fieldNum)), true));
      }
      DataTypes.createStructType(fields);
    }
    catch (Exception e) {
      return new ValidationResult(this, Validity.INVALID, "Flat schema configuration is invalid");
    }

    return new ValidationResult(this, Validity.VALID, "Flat schema configuration is valid");
  }

  @Override
  public Set<String> getKnownPaths() {
    return Sets.newHashSet(this.namesPath, this.typesPath);
  }

}
