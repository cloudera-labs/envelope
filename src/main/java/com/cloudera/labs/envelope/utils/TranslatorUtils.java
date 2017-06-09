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
package com.cloudera.labs.envelope.utils;

import com.typesafe.config.Config;

public class TranslatorUtils {

  public static final String APPEND_RAW_ENABLED_CONFIG_NAME = "append.raw.enabled";
  public static final String APPEND_RAW_KEY_FIELD_NAME_CONFIG_NAME = "append.raw.key.field.name";
  public static final String APPEND_RAW_VALUE_FIELD_NAME_CONFIG_NAME = "append.raw.value.field.name";
  public static final String APPEND_RAW_DEFAULT_KEY_FIELD_NAME = "_key";
  public static final String APPEND_RAW_DEFAULT_VALUE_FIELD_NAME = "_value";
  
  public static boolean doesAppendRaw(Config config) {
    return config.hasPath(APPEND_RAW_ENABLED_CONFIG_NAME) && config.getBoolean(APPEND_RAW_ENABLED_CONFIG_NAME);
  }
  
  public static String getAppendRawKeyFieldName(Config config) {
    if (config.hasPath(APPEND_RAW_KEY_FIELD_NAME_CONFIG_NAME)) {
      return config.getString(APPEND_RAW_KEY_FIELD_NAME_CONFIG_NAME);
    }
    else {
      return APPEND_RAW_DEFAULT_KEY_FIELD_NAME;
    }
  }
  
  public static String getAppendRawValueFieldName(Config config) {
    if (config.hasPath(APPEND_RAW_VALUE_FIELD_NAME_CONFIG_NAME)) {
      return config.getString(APPEND_RAW_VALUE_FIELD_NAME_CONFIG_NAME);
    }
    else {
      return APPEND_RAW_DEFAULT_VALUE_FIELD_NAME;
    }
  }
  
}
