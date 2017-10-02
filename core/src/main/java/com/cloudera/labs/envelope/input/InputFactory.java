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
package com.cloudera.labs.envelope.input;

import com.cloudera.labs.envelope.load.LoadableFactory;
import com.typesafe.config.Config;

public class InputFactory extends LoadableFactory<Input> {

  public static final String TYPE_CONFIG_NAME = "type";

  public static Input create(Config config) {
    if (!config.hasPath(TYPE_CONFIG_NAME)) {
      throw new RuntimeException("Input type not specified");
    }

    String inputType = config.getString(TYPE_CONFIG_NAME);
    Input input = null;
    try {
      input = loadImplementation(Input.class, inputType);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    input.configure(config);

    return input;
  }

}
