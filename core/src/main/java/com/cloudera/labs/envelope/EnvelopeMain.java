/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;

public class EnvelopeMain {

  private static Logger LOG = LoggerFactory.getLogger(EnvelopeMain.class);

  // Entry point to Envelope when submitting directly from spark-submit.
  // Other Java/Scala programs could instead launch an Envelope pipeline by
  // passing their own Config object to Runner#run.
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      throw new RuntimeException("Missing pipeline configuration file argument.");
    } else {
      Path p = Paths.get(args[0]);
      if (Files.notExists(p) || Files.isDirectory(p)) {
        throw new RuntimeException("Can't access pipeline configuration file '" + args[0] + "'.");
      }
    }
    
    LOG.info("Envelope application started");

    Config config = ConfigUtils.configFromPath(args[0]);
    if (args.length == 2) {
      config = ConfigUtils.applySubstitutions(config, args[1]);
    } else if (args.length > 2) {
      LOG.error("Too many parameters to Envelope application");
    } else {
      config = ConfigUtils.applySubstitutions(config);
    }
    LOG.info("Configuration loaded");

    Runner.run(config);
  }
}
