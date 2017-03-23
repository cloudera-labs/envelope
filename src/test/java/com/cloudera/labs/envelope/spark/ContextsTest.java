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
package com.cloudera.labs.envelope.spark;

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.spark.SparkConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ContextsTest {

  private static final String RESOURCES_PATH = "/spark";

  @Test
  public void testSparkPassthroughGood() {
    Config config = ConfigUtils.configFromPath(
      this.getClass().getResource(RESOURCES_PATH + "/spark-passthrough-good.conf").getPath());

    SparkConf sparkConf = Contexts.getSparkConfiguration(config);

    assertTrue(sparkConf.contains("spark.driver.allowMultipleContexts"));
    assertEquals("true", sparkConf.get("spark.driver.allowMultipleContexts"));

    assertTrue(sparkConf.contains("spark.master"));
    assertEquals("local[1]", sparkConf.get("spark.master"));
  }

  @Test
  public void testSparkPassthroughWithInvalid() {
    Config config = ConfigUtils.configFromPath(
      this.getClass().getResource(RESOURCES_PATH + "/spark-passthrough-with-invalid.conf").getPath());

    SparkConf sparkConf = Contexts.getSparkConfiguration(config);

    assertTrue(sparkConf.contains("spark.driver.allowMultipleContexts"));
    assertEquals("true", sparkConf.get("spark.driver.allowMultipleContexts"));

    assertTrue(sparkConf.contains("spark.master"));
    assertEquals("local[1]", sparkConf.get("spark.master"));

    assertFalse(sparkConf.contains("spark.invalid.conf"));
  }

}
