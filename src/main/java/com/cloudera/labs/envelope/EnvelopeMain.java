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
package com.cloudera.labs.envelope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;

public class EnvelopeMain {

  private static Logger LOG = LoggerFactory.getLogger(EnvelopeMain.class);

  public static void main(String[] args) throws Exception {
    LOG.info("Envelope application started");

    Config config = ConfigUtils.configFromPath(args[0]);
    if (args.length == 2) {
      config = ConfigUtils.applySubstitutions(config, args[1]);
    }
    LOG.info("Configuration loaded");

    Runner.run(config);
  }

}
