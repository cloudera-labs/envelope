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

  public static Output create(Config config) throws Exception {
    if (!config.hasPath(TYPE_CONFIG_NAME)) {
      throw new RuntimeException("Output type not specified");
    }

    String outputType = config.getString(TYPE_CONFIG_NAME);

    Output output;

    switch (outputType) {
      case "kudu":
        output = new KuduOutput();
        break;
      case "kafka":
        output = new KafkaOutput();
        break;
      case "log":
        output = new LogOutput();
        break;
      case "hive":
        output = new HiveOutput();
        break;
      case "filesystem":
        output = new FileSystemOutput();
        break;
      case "jdbc":
        output = new JdbcOutput();
        break;
      case "hbase":
        output = new HBaseOutput();
        break;
      default:
        Class<?> clazz = Class.forName(outputType);
        Constructor<?> constructor = clazz.getConstructor();
        output = (Output)constructor.newInstance();
    }

    output.configure(config);

    return output;
  }

}
