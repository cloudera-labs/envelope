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
