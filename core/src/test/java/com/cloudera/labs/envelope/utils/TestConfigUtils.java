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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.ConfigValueType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestConfigUtils {

  @BeforeClass
  public static void envSetup() {
    // This will satisfy IDE unit tests, but not Maven Surefire unit tests
    System.setProperty("substitution.test", "substitution value");
  }

  @Test
  public void testConfigFromPath() throws Exception {
    String configString = "a=1,b.c=X,d.e.f=Y";
    PrintWriter writer = new PrintWriter("testconf.conf", "UTF-8");
    writer.println(configString);
    writer.close();

    Config config = ConfigUtils.configFromPath("testconf.conf");

    Files.delete(Paths.get("testconf.conf"));

    assertEquals(config.getInt("a"), 1);
    assertEquals(config.getString("b.c"), "X");
    assertEquals(config.getString("d.e.f"), "Y");
  }

  @Test
  public void testApplySubstitutionsWithArguments() {
    Config baseConfig = ConfigFactory.parseString("key_a = ${a}, key_b = ${b}, key_c = ${c}, key_d = ${substitution.test}");

    String substitutions = "a=1,b=X,c=Y";

    Config substitutedConfig = ConfigUtils.applySubstitutions(baseConfig, substitutions);

    assertEquals(substitutedConfig.getInt("key_a"), 1);
    assertEquals(substitutedConfig.getString("key_b"), "X");
    assertEquals(substitutedConfig.getString("key_c"), "Y");
    assertEquals(substitutedConfig.getString("key_d"), "substitution value");
  }

  @Test
  public void testApplySubstitutionsNoArguments() {
    Config baseConfig = ConfigFactory.parseString("key_a = A, key_b = ${substitution.test}, key_c = ${key_a}");
    Config substitutedConfig = ConfigUtils.applySubstitutions(baseConfig);

    assertEquals(substitutedConfig.getString("key_a"), "A");
    assertEquals(substitutedConfig.getString("key_b"), "substitution value");
    assertEquals(substitutedConfig.getString("key_c"), "A");
  }

  @Test
  public void testOptionMap() {
    Config config1 = ConfigFactory.parseString("key_a: 1");
    Config config2 = ConfigFactory.parseString("key_a: two");

    ConfigUtils.OptionMap optionMap1 = new ConfigUtils.OptionMap(config1);
    optionMap1.resolve("option", "key_a");
    optionMap1.resolve("none", "foo");

    ConfigUtils.OptionMap optionMap2 = new ConfigUtils.OptionMap(config2);
    optionMap2.resolve("option", "key_a");

    assertNull("Invalid option value", optionMap1.get("none"));
    assertNotSame("OptionMaps are the same", optionMap1.get("option"), optionMap2.get("option"));
  }
  
  @Test
  public void testFindReplaceStringValues() {
    Config baseConfig = ConfigFactory.parseString("a: ${replaceme}, b: \"${replaceme}\", c: [${replaceme}, ${replaceme}], d: [\"${replaceme}\", \"${replaceme}\"], e: { f: \"${replaceme}\", g: [\"${replaceme}\"] }" );
    Config resolvedConfig = baseConfig.resolveWith(ConfigFactory.empty().withValue("replaceme", ConfigValueFactory.fromAnyRef("REPLACED")));
    Config replacedConfig = ConfigUtils.findReplaceStringValues(resolvedConfig, "\\$\\{replaceme\\}", "REPLACED");
  
    assertEquals(replacedConfig.getString("a"), "REPLACED");
    assertEquals(replacedConfig.getString("b"), "REPLACED");
    assertEquals(replacedConfig.getStringList("c").get(0), "REPLACED");
    assertEquals(replacedConfig.getStringList("c").get(1), "REPLACED");
    assertEquals(replacedConfig.getStringList("d").get(0), "REPLACED");
    assertEquals(replacedConfig.getStringList("d").get(1), "REPLACED");
    assertEquals(replacedConfig.getConfig("e").getString("f"), "REPLACED");
    assertEquals(replacedConfig.getConfig("e").getStringList("g").get(0), "REPLACED");
  }

  @Test
  public void testCanBeCoerced() {
    Config config = ConfigFactory.empty().withValue("hello", ConfigValueFactory.fromAnyRef("true"));
    assertTrue(ConfigUtils.canBeCoerced(config, "hello", ConfigValueType.BOOLEAN));
  }

  @Test
  public void testCanNotBeCoerced() {
    Config config = ConfigFactory.empty().withValue("hello", ConfigValueFactory.fromAnyRef("maybe?"));
    assertFalse(ConfigUtils.canBeCoerced(config, "hello", ConfigValueType.BOOLEAN));
  }

  @Test
  public void getOrElseExists() {
    Config config = ConfigFactory.empty().withValue("hello", ConfigValueFactory.fromAnyRef(true));
    assertEquals(true, ConfigUtils.getOrElse(config, "hello", false));
  }

  @Test
  public void getOrElseNotExists() {
    Config config = ConfigFactory.empty().withValue("hello", ConfigValueFactory.fromAnyRef(true));
    assertEquals(false, ConfigUtils.getOrElse(config, "world", false));
  }

}
