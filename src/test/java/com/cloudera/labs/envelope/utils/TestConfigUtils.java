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

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestConfigUtils {

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
  public void testApplySubstitutions() {
    Config baseConfig = ConfigFactory.parseString("key_a = ${a}, key_b = ${b}, key_c = ${c}");

    String substitutions = "a=1,b=X,c=Y";

    Config substitutedConfig = ConfigUtils.applySubstitutions(baseConfig, substitutions);

    assertEquals(substitutedConfig.getInt("key_a"), 1);
    assertEquals(substitutedConfig.getString("key_b"), "X");
    assertEquals(substitutedConfig.getString("key_c"), "Y");
  }

}
