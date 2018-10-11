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

package com.cloudera.labs.envelope.input.translate;

import com.cloudera.labs.envelope.load.LoadableFactory;
import com.typesafe.config.Config;

public class TranslatorFactory extends LoadableFactory<Translator<?,?>> {

  public static final String TYPE_CONFIG_NAME = "type";

  public static Translator<?, ?> create(Config config, boolean configure) {
    if (!config.hasPath(TYPE_CONFIG_NAME)) {
      throw new RuntimeException("Translator type not specified");
    }

    String translatorType = config.getString(TYPE_CONFIG_NAME);
    Translator<?, ?> translator;
    try {
      translator = loadImplementation(Translator.class, translatorType);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    if (configure) {
      translator.configure(config);
    }

    return translator;
  }

}
