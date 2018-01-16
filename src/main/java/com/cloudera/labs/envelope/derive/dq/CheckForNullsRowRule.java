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


import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CheckForNullsRowRule implements RowRule {

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

}
