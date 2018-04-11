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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;

public class RegexRowRule implements RowRule, ProvidesAlias {

  private static final String REGEX_CONFIG = "regex";
  private static final String FIELDS_CONFIG = "fields";

  private String name;
  private Pattern pattern;
  private List<String> fields;

  @Override
  public void configure(String name, Config config) {
    this.name = name;
    ConfigUtils.assertConfig(config, REGEX_CONFIG);
    ConfigUtils.assertConfig(config, FIELDS_CONFIG);

    String regex = config.getString(REGEX_CONFIG);
    pattern = Pattern.compile(regex);
    fields = config.getStringList(FIELDS_CONFIG);
  }

  @Override
  public boolean check(Row row) {
    boolean check = true;
    for (String field : fields) {
      String value = row.getAs(field);
      if (value != null) {
        Matcher matcher = pattern.matcher(value);
        check = check && matcher.matches();
      } else {
          check = false;
      }
      if (!check) {
        // No point continuing if failed
        break;
      }
    }
    return check;
  }

  @Override
  public String getAlias() {
    return "regex";
  }
}
