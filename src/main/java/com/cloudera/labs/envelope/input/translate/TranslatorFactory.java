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
package com.cloudera.labs.envelope.input.translate;

import java.lang.reflect.Constructor;

import com.typesafe.config.Config;

public class TranslatorFactory {

  public static final String TYPE_CONFIG_NAME = "type";

  public static Translator<?, ?> create(Config config) {
    if (!config.hasPath(TYPE_CONFIG_NAME)) {
      throw new RuntimeException("Translator type not specified");
    }

    String translatorType = config.getString(TYPE_CONFIG_NAME);

    Translator<?,?> translator = null;

    if (translatorType.equals("kvp")) {
      translator = new KVPTranslator();
    }
    else if (translatorType.equals("delimited")) {
      translator = new DelimitedTranslator();
    }
    else if (translatorType.equals("avro")) {
      translator = new AvroTranslator();
    }
    else if (translatorType.equals("morphline")) {
      translator = new MorphlineTranslator<>();
    }
    else {
      try {
        Class<?> clazz = Class.forName(translatorType);
        Constructor<?> constructor = clazz.getConstructor();
        translator = (Translator<?, ?>)constructor.newInstance();
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    translator.configure(config);

    return translator;
  }

}
