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

package com.cloudera.labs.envelope.configuration;

import com.cloudera.labs.envelope.component.Component;
import com.typesafe.config.Config;

/**
 * Config loaders retrieve additional configuration to be merged into the base configuration
 * provided when the Envelope application is run.
 *
 * The config loader will be run before the pipeline begins. For streaming pipelines the
 * config loader will also be run every micro-batch, but in these calls only configuration that
 * matches steps that are dependent (directly or indirectly) on a stream input will be merged
 * into the micro-batch.
 */
public interface ConfigLoader extends Component {

  /**
   * Get the configuration to be merged into the base configuration.
   */
  Config getConfig();

}
