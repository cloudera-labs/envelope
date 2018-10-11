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
import com.cloudera.labs.envelope.validate.Validation;
import com.cloudera.labs.envelope.validate.ValidationResult;
import com.cloudera.labs.envelope.validate.Validations;
import com.cloudera.labs.envelope.validate.Validity;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class RegexRowRule implements RowRule, ProvidesAlias, ProvidesValidations {

  private static final String REGEX_CONFIG = "regex";
  private static final String FIELDS_CONFIG = "fields";

  private String name;
  private Pattern pattern;
  private List<String> fields;

  @Override
  public void configure(String name, Config config) {
    this.name = name;

    String regex = config.getString(REGEX_CONFIG);
    pattern = Pattern.compile(regex);
    fields = config.getStringList(FIELDS_CONFIG);
  }

  @Override
  public boolean check(Row row) {
    boolean check = true;
    for (String field : fields) {
      String value = row.getAs(field);
      Matcher matcher = pattern.matcher(value);
      check = check && matcher.matches();
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

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(REGEX_CONFIG, ConfigValueType.STRING)
        .mandatoryPath(FIELDS_CONFIG, ConfigValueType.LIST)
        .add(new Validation() {
          @Override
          public ValidationResult validate(Config config) {
            try {
              Pattern.compile(config.getString(REGEX_CONFIG));
            }
            catch (PatternSyntaxException pse) {
              return new ValidationResult(this, Validity.INVALID, "Regular expression does not have valid syntax");
            }
            return new ValidationResult(this, Validity.VALID, "Regular expression has valid syntax");
          }
          @Override
          public Set<String> getKnownPaths() {
            return Sets.newHashSet(REGEX_CONFIG);
          }
        })
        .build();
  }
  
}
