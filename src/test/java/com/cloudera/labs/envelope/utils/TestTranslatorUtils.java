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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.cloudera.labs.envelope.utils.TranslatorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestTranslatorUtils {

  @Test
  public void testDoesAppendRaw() {
    Config config = ConfigFactory.empty().withValue(
        TranslatorUtils.APPEND_RAW_ENABLED_CONFIG_NAME, ConfigValueFactory.fromAnyRef(true));
    
    assertTrue(TranslatorUtils.doesAppendRaw(config));
  }
  
  @Test
  public void testDefaultsNotAppendRaw() {
    Config config = ConfigFactory.empty();
    
    assertFalse(TranslatorUtils.doesAppendRaw(config));
  }

  @Test
  public void testSpecifiesNotAppendRaw() {
    Config config = ConfigFactory.empty().withValue(
        TranslatorUtils.APPEND_RAW_ENABLED_CONFIG_NAME, ConfigValueFactory.fromAnyRef(false));
    
    assertFalse(TranslatorUtils.doesAppendRaw(config));
  }

  @Test
  public void testGetSpecifiedAppendRawKeyFieldName() {
    Config config = ConfigFactory.empty().withValue(
        TranslatorUtils.APPEND_RAW_KEY_FIELD_NAME_CONFIG_NAME, ConfigValueFactory.fromAnyRef("hello"));
    
    assertEquals(TranslatorUtils.getAppendRawKeyFieldName(config), "hello");
  }
  
  @Test
  public void testGetSpecifiedAppendRawValueFieldName() {
    Config config = ConfigFactory.empty().withValue(
        TranslatorUtils.APPEND_RAW_VALUE_FIELD_NAME_CONFIG_NAME, ConfigValueFactory.fromAnyRef("world"));
    
    assertEquals(TranslatorUtils.getAppendRawValueFieldName(config), "world");
  }
  
  @Test
  public void testGetDefaultAppendRawKeyFieldName() {
    Config config = ConfigFactory.empty();
    
    assertEquals(TranslatorUtils.getAppendRawKeyFieldName(config), TranslatorUtils.APPEND_RAW_DEFAULT_KEY_FIELD_NAME);
  }
  
  @Test
  public void testGetDefaultAppendRawValueFieldName() {
    Config config = ConfigFactory.empty();
    
    assertEquals(TranslatorUtils.getAppendRawValueFieldName(config), TranslatorUtils.APPEND_RAW_DEFAULT_VALUE_FIELD_NAME);
  }
  
}
