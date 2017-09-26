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
package com.cloudera.labs.envelope.output;

import java.lang.reflect.Constructor;

import com.typesafe.config.Config;

public class OutputFactory {

  public static final String TYPE_CONFIG_NAME = "type";

  public static Output create(Config config) {
    if (!config.hasPath(TYPE_CONFIG_NAME)) {
      throw new RuntimeException("Output type not specified");
    }

    String outputType = config.getString(TYPE_CONFIG_NAME);

    String outputClass;
    Output output;

    switch (outputType) {
      case "kudu":
        outputClass = "com.cloudera.labs.envelope.output.KuduOutput";
        break;
      case "kafka":
        outputClass = "com.cloudera.labs.envelope.output.KafkaOutput";
        break;
      case "log":
        outputClass = "com.cloudera.labs.envelope.output.LogOutput";
        break;
      case "hive":
        outputClass = "com.cloudera.labs.envelope.output.HiveOutput";
        break;
      case "filesystem":
        outputClass = "com.cloudera.labs.envelope.output.FileSystemOutput";
        break;
      case "jdbc":
        outputClass = "com.cloudera.labs.envelope.output.JdbcOutput";
        break;
      case "hbase":
        outputClass = "com.cloudera.labs.envelope.output.HBaseOutput";
        break;
      case "zookeeper":
        outputClass = "com.cloudera.labs.envelope.output.ZooKeeperOutput";
        break;
      default:
        outputClass = outputType;
    }
    
    try {
      Class<?> clazz = Class.forName(outputClass);
      Constructor<?> constructor = clazz.getConstructor();
      output = (Output)constructor.newInstance();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    output.configure(config);

    return output;
  }

}
