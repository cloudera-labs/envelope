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
package com.cloudera.labs.envelope.derive;

import java.lang.reflect.Constructor;

import com.typesafe.config.Config;

public class DeriverFactory {

  public static final String TYPE_CONFIG_NAME = "type";

  public static Deriver create(Config config) {
    if (!config.hasPath(TYPE_CONFIG_NAME)) {
      throw new RuntimeException("Deriver type not specified");
    }

    String deriverType = config.getString(TYPE_CONFIG_NAME);

    Deriver deriver;

    switch (deriverType) {
      case "sql":
        deriver = new SQLDeriver();
        break;
      case "passthrough":
        deriver = new PassthroughDeriver();
        break;
      case "nest":
        deriver = new NestDeriver();
        break;
      case "morphline":
        deriver = new MorphlineDeriver();
        break;
      case "pivot":
        deriver = new PivotDeriver();
        break;
      default:
        try {
          Class<?> clazz = Class.forName(deriverType);
          Constructor<?> constructor = clazz.getConstructor();
          deriver = (Deriver) constructor.newInstance();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
    }

    deriver.configure(config);

    return deriver;
  }

}
