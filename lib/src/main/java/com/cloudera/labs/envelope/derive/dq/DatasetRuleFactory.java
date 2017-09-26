/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.derive.dq;

import com.typesafe.config.Config;

import java.lang.reflect.Constructor;

public class DatasetRuleFactory {

  private static final String TYPE_CONFIG_NAME = "type";

  public static DatasetRule create(String name, Config config) {
    if (!config.hasPath(TYPE_CONFIG_NAME)) {
      throw new RuntimeException("Rule type not specified");
    }

    String ruleType = config.getString(TYPE_CONFIG_NAME);

    DatasetRule rule;

    switch (ruleType) {
      case "count":
        rule = new CountDatasetRule();
        break;
      case "checkschema":
        rule = new CheckSchemaDatasetRule();
        break;
      case "checknulls":
      case "range":
      case "enum":
      case "regex":
        rule = new DatasetRowRuleWrapper();
        break;
      default:
        try {
          Class<?> clazz = Class.forName(ruleType);
          Constructor<?> constructor = clazz.getConstructor();
          Object rawRule = constructor.newInstance();
          if (rawRule instanceof RowRule) {
            rule = new DatasetRowRuleWrapper();
          } else {
            rule = (DatasetRule) constructor.newInstance();
          }
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
    }

    rule.configure(name, config);

    return rule;
  }

}
