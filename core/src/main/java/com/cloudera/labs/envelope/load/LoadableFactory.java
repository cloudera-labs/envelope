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

package com.cloudera.labs.envelope.load;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public abstract class LoadableFactory<T> {

  public static final String TYPE_CONFIG_NAME = "type";

  /**
   * Get a map of alias to FQDN for the supplied base type. Classes should be specified in the
   * Java ServiceLoader file in META-INF.
   * @param clazz - a class implementing {@link Loadable}
   * @return - a map of alias to FQDN for all loadable classes
   */
  protected synchronized static <T> Map<String, String> getLoadables(Class<T> clazz) {
    ServiceLoader<T> loader = ServiceLoader.load(clazz);
    Map<String, String> loadableMap = new HashMap<>();
    for (T loadable : loader) {
      if (loadable instanceof ProvidesAlias) {
        ProvidesAlias loadableWithAlias = (ProvidesAlias)loadable;
        if (loadableMap.containsKey(loadableWithAlias.getAlias())) {
          throw new RuntimeException("More than one loadable with alias: " + loadableWithAlias.getAlias());
        }
        loadableMap.put(loadableWithAlias.getAlias(), loadableWithAlias.getClass().getCanonicalName());
      }
    }
    return loadableMap;
  }

  /**
   * Get a map of alias to FQDN for the supplied base type. Classes should be specified in the
   * Java ServiceLoader file in META-INF.
   * @param aliasOrClassName - a class implementing/extending the baseClass type
   * @return - a map of alias to FQDN for all loadable classes
   */
  protected static <T> T loadImplementation(Class<T> baseClass, String aliasOrClassName)
      throws ClassNotFoundException {
    String actualClazz = aliasOrClassName;

    for (Map.Entry<String, String> alias : getLoadables(baseClass).entrySet()) {
      if (aliasOrClassName.equals(alias.getKey())) {
        actualClazz = alias.getValue();
      }
    }

    T t;
    try {
      Class<?> clazz = Class.forName(actualClazz);
      Constructor<?> constructor = clazz.getConstructor();
      t = (T) constructor.newInstance();
    } catch (ClassNotFoundException|ClassCastException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return t;
  }

}
