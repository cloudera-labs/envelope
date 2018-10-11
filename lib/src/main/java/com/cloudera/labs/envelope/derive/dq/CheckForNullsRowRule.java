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
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class CheckForNullsRowRule implements RowRule, ProvidesAlias, ProvidesValidations {

  private static final String FIELDS_CONFIG = "fields";

  private List<String> fields = new ArrayList<>();

  @Override
  public void configure(String name, Config config) {
    this.fields = config.getStringList(FIELDS_CONFIG);
  }

  @Override
  public boolean check(Row row) {
    for (String field : fields) {
      if (row.isNullAt(row.fieldIndex(field))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String getAlias() {
    return "checknulls";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(FIELDS_CONFIG, ConfigValueType.LIST)
        .build();
  }
  
}
