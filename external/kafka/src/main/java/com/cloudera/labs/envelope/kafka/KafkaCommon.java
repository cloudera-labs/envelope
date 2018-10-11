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

package com.cloudera.labs.envelope.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaCommon {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaCommon.class.getName());

  public static final String PARAMETER_CONFIG_PREFIX = "parameter.";

  /**
   * Add custom parameters to the configuration key-value map
   * @param params map to which to add new parameters
   * @param config config for the input/output
   */
  public static void addCustomParams(Map<String, Object> params, Config config) {
    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      String propertyName = entry.getKey();
      if (propertyName.startsWith(PARAMETER_CONFIG_PREFIX)) {
        String paramName = propertyName.substring(PARAMETER_CONFIG_PREFIX.length());
        String paramValue = config.getString(propertyName);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding Kafka property: {} = \"{}\"", paramName, paramValue);
        }
        params.put(paramName, paramValue);
      }
    }
  }

}
