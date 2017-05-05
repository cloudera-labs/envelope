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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.HashMap;
import java.util.regex.Pattern;

public class ConfigUtils {

  public static Config configFromPath(String path) {
    File configFile = new File(path);
    return ConfigFactory.parseFile(configFile);
  }

  public static Config applySubstitutions(Config config) {
    return ConfigFactory.defaultOverrides()
        .withFallback(config)
        .resolve();
  }

  public static Config applySubstitutions(Config config, String substitutionsString) {
    String[] substitutions = substitutionsString.split(Pattern.quote(","));

    for (String substitution : substitutions) {
      Config substitutionConfig = ConfigFactory.parseString(substitution);
      config = config.withFallback(substitutionConfig);
    }

    return applySubstitutions(config);
  }

  public static class OptionMap extends HashMap<String, String> {
    private Config config;

    public OptionMap(Config config) {
      this.config = config;
    }

    public OptionMap resolve(String option, String parameter) {
      if (config.hasPath(parameter)) {
        this.put(option, config.getString(parameter));
      }
      return this;
    }
  }

}
