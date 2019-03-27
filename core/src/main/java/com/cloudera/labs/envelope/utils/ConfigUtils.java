/*
 * Copyright (c) 2015-2019, Cloudera, Inc. All Rights Reserved.
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

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.configuration.ConfigLoader;
import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.ConfigValueType;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ConfigUtils {

  public static final String JAR_FILE_EXCEPTION_MESSAGE = "A jar file has been provided as the" +
      "second argument to the Spark submit call, instead of the Envelope configuration file. " +
      "If the jar argument contains a wildcard then it is likely expanding out to multiple" +
      "filenames, and the single Envelope jar file should be specified instead";

  public static Config configFromPath(String path) {
    File configFile = new File(path);

    Config config;
    try {
      config = ConfigFactory.parseFile(configFile);
    }
    catch (ConfigException.Parse e) {
      // The tell-tale sign is the magic bytes "PK" that is at the start of all jar files
      if (e.getMessage().contains("Key '\"PK")) {
        throw new RuntimeException(JAR_FILE_EXCEPTION_MESSAGE);
      }
      else {
        throw e;
      }
    }

    return config;
  }

  public static Config configFromResource(String resource) {
    try (Reader reader = new InputStreamReader(ConfigUtils.class.getResourceAsStream(resource))) {
      return ConfigFactory.parseReader(reader);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  public static Config getApplicationConfig(Config config) {
    return getOrElse(config, Contexts.APPLICATION_SECTION_PREFIX, ConfigFactory.empty());
  }

  public static Config mergeLoadedConfiguration(Config config) {
    if (getApplicationConfig(config).hasPath(Runner.CONFIG_LOADER_PROPERTY)) {
      Config configLoaderConfig = getApplicationConfig(config).getConfig(Runner.CONFIG_LOADER_PROPERTY);
      ConfigLoader configLoader = ComponentFactory.create(ConfigLoader.class, configLoaderConfig, true);
      Config loadedConfig = configLoader.getConfig();

      return loadedConfig.withFallback(config).resolve();
    }
    else {
      return config;
    }
  }

  @SuppressWarnings("serial")
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
  
  public static Config findReplaceStringValues(Config config, String findRegex, Object replace) {
    for (Map.Entry<String, ConfigValue> valueEntry : config.entrySet()) {
      ConfigValueType valueType = valueEntry.getValue().valueType();
      if (valueType.equals(ConfigValueType.OBJECT)) {
        config = ConfigUtils.findReplaceStringValues(config.getConfig(valueEntry.getKey()), findRegex, replace);
      }
      else if (valueType.equals(ConfigValueType.LIST)) {
        @SuppressWarnings("unchecked")
        List<Object> valueList = (List<Object>)valueEntry.getValue().unwrapped();
        if (valueList.size() > 0) {
          if (valueList.get(0) instanceof String) {
            for (int i = 0; i < valueList.size(); i++) {
              String found = (String)valueList.get(i);
              String replaced = found.replaceAll(findRegex, replace.toString());
              valueList.set(i, replaced);
            }
          }
        }
        config = config.withValue(valueEntry.getKey(), ConfigValueFactory.fromAnyRef(valueList));
      }
      else if (valueType.equals(ConfigValueType.STRING)) {
        String found = (String)valueEntry.getValue().unwrapped();
        String replaced = found.replaceAll(findRegex, replace.toString());
        config = config.withValue(valueEntry.getKey(), ConfigValueFactory.fromAnyRef(replaced));
      }
    }
    
    return config;
  }

  public static boolean canBeCoerced(Config config, String path, ConfigValueType type) {
    if (type == ConfigValueType.BOOLEAN) {
      try {
        config.getBoolean(path);
      }
      catch (ConfigException.WrongType e) {
        return false;
      }
    }
    else {
      // Other data type coercions could be added here in the future
      return false;
    }

    return true;
  }

  public static <T> T getOrElse(Config config, String path, T orElse) {
    if (config.hasPath(path)) {
      if (config.getValue(path).valueType() == ConfigValueType.OBJECT) {
        return (T) config.getConfig(path);
      } else {
        return (T) config.getAnyRef(path);
      }
    }
    else {
      return orElse;
    }
  }

  public static <T> T getOrNull(Config config, String path) {
    return getOrElse(config, path, null);
  }

}
