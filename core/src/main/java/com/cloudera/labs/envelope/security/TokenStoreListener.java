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

package com.cloudera.labs.envelope.security;

import static com.cloudera.labs.envelope.security.SecurityUtils.SECURITY_PREFIX;
import static com.cloudera.labs.envelope.security.SecurityUtils.TOKENS_CHECK_INTERVAL;
import static com.cloudera.labs.envelope.spark.Contexts.ENVELOPE_CONFIGURATION_SPARK;

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.token.Token;
import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to watch a specified path on a Hadoop {@link FileSystem} for TokenStore files
 * matching a specified pattern. On detection of a new file, it is read and the tokens are
 * extracted.
 *
 * Clients of the class can check to see if a token has been updated using the {@code hasChanged(String alias)} method
 * and retrieve desired tokens by alias using the {@code getToken(String alias)} method.
 *
 */
public class TokenStoreListener {

  private static final Logger LOG = LoggerFactory.getLogger(TokenStoreListener.class);

  private static final int DEFAULT_REFRESH_INTERVAL_SECONDS = 60;

  private static TokenStoreListener INSTANCE;

  private static Config config;
  private static Path latestTokenStorePath;
  private static TokenStore tokenStore;
  private static ConcurrentHashMap<String, TokenWrapper> tokenWrapperMap = new ConcurrentHashMap<>();
  private static long listenerCheckIntervalMillis = DEFAULT_REFRESH_INTERVAL_SECONDS * 1000;

  private TokenStoreListener(Config conf) {
    config = conf;
    if (conf.hasPath(TOKENS_CHECK_INTERVAL)) {
      listenerCheckIntervalMillis = conf.getDuration(TOKENS_CHECK_INTERVAL,
          TimeUnit.MILLISECONDS);
    }

    checkForUpdatedTokenStore();
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    executorService.scheduleAtFixedRate(new TokenRenewer(), listenerCheckIntervalMillis,
        listenerCheckIntervalMillis, TimeUnit.MILLISECONDS);
  }

  public Token getToken(String alias) {
    if (tokenWrapperMap.containsKey(alias)) {
      LOG.trace("Token {} accessed", alias);
      return tokenWrapperMap.get(alias).getToken();
    } else {
      throw new RuntimeException("Token not found in tokenStore file for " + alias);
    }
  }

  public boolean hasChanged(String alias) {
    if (tokenWrapperMap.containsKey(alias)) {
      return tokenWrapperMap.get(alias).hasChanged();
    } else {
      LOG.warn("No token for [{}], returning false for hasChanged", alias);
      return false;
    }
  }

  // I don't like this implementation, but there isn't another way unfortunately as the Credentials object doesn't let
  // us list the hadoop token tokenAliases it contains.
  private static void populateHadoopToken(String alias) {
    LOG.debug("Adding token with alias {} to internal store", alias);
    Token token = tokenStore.getToken(alias);
    if (token == null) {
      LOG.error("Credentials file does not contain a token for [{}]", alias);
      throw new RuntimeException("Non-existent security token requested");
    }

    // The TokenWrapper map lets us keep track of when each token was last accessed to see if it has changed.
    if (!tokenWrapperMap.containsKey(alias)) {
      tokenWrapperMap.put(alias, new TokenWrapper(token));
    } else {
      tokenWrapperMap.get(alias).setToken(token);
    }
  }

  private static class TokenWrapper {

    private long updated;
    private long lastAccessed = 0;
    private Token token;

    TokenWrapper(Token token) {
      this.token = token;
      setUpdateTime();
    }

    Token getToken() {
      setAccessTime();
      return token;
    }

    void setToken(Token token) {
      if (!this.token.equals(token)) {
        this.token = token;
        setUpdateTime();
      }
    }

    boolean hasChanged() {
      return updated > lastAccessed;
    }

    long getLastAccessed() {
      return lastAccessed;
    }

    private void setAccessTime() {
      lastAccessed = System.currentTimeMillis();
    }

    private void setUpdateTime() {
      updated = System.currentTimeMillis();
    }

  }

  @SuppressWarnings("unchecked")
  private static void checkForUpdatedTokenStore() {
    LOG.debug("Checking for tokenStore file updates");
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      List<Path> tokenFiles = SecurityUtils.getExistingTokenStoreFiles(config, fs.getConf(), false);
      LOG.debug("Found {} tokenstore candidate(s)", tokenFiles.size());

      if (!tokenFiles.isEmpty()) {
        Collections.sort(tokenFiles);
        Path latestCandidate = null;
        boolean foundValidCandidate = false;
        for (int i = tokenFiles.size() - 1; i >= 0 && !foundValidCandidate; i--) {
          latestCandidate = tokenFiles.get(i);
          if (fs.exists(new Path(latestCandidate.toString() + ".READY"))) {
            foundValidCandidate = true;
          }
        }

        if (foundValidCandidate && !latestCandidate.equals(latestTokenStorePath)) {
          LOG.debug("New tokenStore file found at {}", latestCandidate);

          // Read in token store file
          tokenStore = new TokenStore();
          tokenStore.read(tokenFiles.get(tokenFiles.size() - 1).toString(), fs.getConf());
          latestTokenStorePath = latestCandidate;

          for (String alias : tokenStore.getTokenAliases()) {
            populateHadoopToken(alias);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read token store: " + e.getMessage());
    }
  }

  public static class TokenRenewer implements Runnable {

    @Override
    public void run() {
      checkForUpdatedTokenStore();
    }

  }

  public synchronized static TokenStoreListener get() {
    if (INSTANCE == null) {
      LOG.trace("SparkConf: " + SparkEnv.get().conf().toDebugString());
      Config config = ConfigFactory.parseString(SparkEnv.get().conf().get(ENVELOPE_CONFIGURATION_SPARK));
      INSTANCE = new TokenStoreListener(ConfigUtils.getOrElse(config, SECURITY_PREFIX, ConfigFactory.empty()));
    }
    return INSTANCE;
  }

}
