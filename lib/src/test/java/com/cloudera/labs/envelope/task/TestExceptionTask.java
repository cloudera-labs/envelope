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

package com.cloudera.labs.envelope.task;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.Properties;

public class TestExceptionTask {

  @Test (expected = RuntimeException.class)
  public void testException() {
    Properties configProps = new Properties();
    configProps.setProperty(ExceptionTask.MESSAGE_CONFIG, "I meant this!");
    Config config = ConfigFactory.parseProperties(configProps);

    Task task = new ExceptionTask();
    task.configure(config);

    task.run(Maps.<String, Dataset<Row>>newHashMap());
  }

}
