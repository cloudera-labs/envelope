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

package com.cloudera.labs.envelope.utils;

import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;

public class TranslatorUtils {

  public static final String APPEND_RAW_ENABLED_CONFIG_NAME = "append.raw.enabled";
  public static final String APPEND_RAW_KEY_FIELD_NAME_CONFIG_NAME = "append.raw.key.field.name";
  public static final String APPEND_RAW_VALUE_FIELD_NAME_CONFIG_NAME = "append.raw.value.field.name";
  public static final String APPEND_RAW_DEFAULT_KEY_FIELD_NAME = "_key";
  public static final String APPEND_RAW_DEFAULT_VALUE_FIELD_NAME = "_value";

  public static final Validations APPEND_RAW_VALIDATIONS = Validations.builder()
      .optionalPath(APPEND_RAW_ENABLED_CONFIG_NAME, ConfigValueType.BOOLEAN)
      .ifPathHasValue(APPEND_RAW_ENABLED_CONFIG_NAME, true,
          Validations.single().optionalPath(APPEND_RAW_DEFAULT_KEY_FIELD_NAME, ConfigValueType.STRING))
      .ifPathHasValue(APPEND_RAW_ENABLED_CONFIG_NAME, true,
          Validations.single().optionalPath(APPEND_RAW_DEFAULT_VALUE_FIELD_NAME, ConfigValueType.STRING))
      .build();
  
  public static boolean doesAppendRaw(Config config) {
    return ConfigUtils.getOrElse(config, APPEND_RAW_ENABLED_CONFIG_NAME, false);
  }
  
  public static String getAppendRawKeyFieldName(Config config) {
    return ConfigUtils.getOrElse(config, APPEND_RAW_KEY_FIELD_NAME_CONFIG_NAME, APPEND_RAW_DEFAULT_KEY_FIELD_NAME);
  }
  
  public static String getAppendRawValueFieldName(Config config) {
    return ConfigUtils.getOrElse(config, APPEND_RAW_VALUE_FIELD_NAME_CONFIG_NAME, APPEND_RAW_DEFAULT_VALUE_FIELD_NAME);
  }
  
}
