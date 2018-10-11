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

import static com.cloudera.labs.envelope.security.SecurityUtils.TOKENS_CHECK_INTERVAL;
import static com.cloudera.labs.envelope.security.SecurityUtils.TOKENS_DELETION_DELAY;
import static com.cloudera.labs.envelope.security.SecurityUtils.TOKENS_RENEW_FACTOR;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenStoreManager {

  private static final Logger LOG = LoggerFactory.getLogger(TokenStoreManager.class);

  public static final long ONE_DAY_MILLIS = 86400 * 1000;

  private static final int DEFAULT_REFRESH_INTERVAL_SECONDS = 60;

  private static final int DEFAULT_DELETION_DELAY_SECONDS = 2 * DEFAULT_REFRESH_INTERVAL_SECONDS;

  private Config config;
  private TokenStore tokenStore;
  private ConcurrentHashMap<String, ExpiringTokenProvider> tokenProviders = new ConcurrentHashMap<>();
  private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
  private long providerCheckIntervalMillis = DEFAULT_REFRESH_INTERVAL_SECONDS * 1000;
  private long oldTokenStoreFileDeletionDelay = DEFAULT_DELETION_DELAY_SECONDS * 1000;
  private double tokenRenewIntervalFactor = 0.8;
  private Configuration hadoopConf = new Configuration();

  private AtomicBoolean isStarted = new AtomicBoolean(false);

  public TokenStoreManager(Config inputConfig) {
    config = inputConfig;

    if (config.hasPath(TOKENS_CHECK_INTERVAL)) {
      providerCheckIntervalMillis = config.getDuration(TOKENS_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    }
    if (config.hasPath(TOKENS_RENEW_FACTOR)) {
      tokenRenewIntervalFactor = config.getDouble(TOKENS_RENEW_FACTOR);
    }
    if (config.hasPath(TOKENS_DELETION_DELAY)) {
      oldTokenStoreFileDeletionDelay = config.getDuration(TOKENS_DELETION_DELAY, TimeUnit.MILLISECONDS);
    }

    tokenStore = new TokenStore();
  }

  /**
   * Register a security provider implementation. The manager will call this class to obtain new tokens according
   * to the renewal interval it specifies
   * @param provider a {@link TokenProvider} implementation
   */
  public void addTokenProvider(TokenProvider provider) throws Exception {
    Preconditions.checkNotNull(provider, "Supplied TokenProvider cannot be null");
    String alias = provider.getAlias();
    if (!tokenProviders.containsKey(alias)) {
      LOG.info("Adding security provider for [{}]", alias);
      // Get the first token and add to the renewal map
      obtainAndRegisterToken(alias, provider);
      tokenProviders.put(alias, new ExpiringTokenProvider(provider, this));
    }
  }

  private void addOrReplaceTokenForAlias(String alias, Token token) {
    tokenStore.addToken(alias, token);
  }

  private void obtainAndRegisterToken(String alias, TokenProvider provider) throws Exception {
    addOrReplaceTokenForAlias(alias, provider.obtainToken());
  }

  private void writeTokenStore() {
    try {
      String tokenFilePath = String.format("%s.%d", SecurityUtils.getTokenStoreFilePath(config, true),
          System.currentTimeMillis());
      LOG.debug("Writing token store to {}", tokenFilePath);
      // Get existing token store files
      List<Path> existing = SecurityUtils.getExistingTokenStoreFiles(config, hadoopConf, true);
      LOG.debug("{} existing token store files", existing.size());

      // Write the new version
      tokenStore.write(tokenFilePath, hadoopConf);
      LOG.info("Written new token store file to {}", tokenFilePath);

      // Remove all the old ones
      SecurityUtils.deleteTokenStoreFiles(existing, oldTokenStoreFileDeletionDelay, hadoopConf);
    } catch (IOException e) {
      throw new RuntimeException("Could not write tokens: " +
          e.getMessage());
    }
  }

  private void cleanupTokenStoreFiles() {
    try {
      List<Path> tokenStoreFiles = SecurityUtils.getExistingTokenStoreFiles(config, hadoopConf, true);
      SecurityUtils.deleteTokenStoreFiles(tokenStoreFiles, 0, hadoopConf);
    } catch (IOException e) {
      LOG.error("Could not list the temporary tokenStore files: {}", e.getMessage());
    }
  }

  public static class ExpiringTokenProvider {

    TokenProvider provider;
    private long creationTime;
    private long renewalInterval;

    ExpiringTokenProvider(TokenProvider provider, TokenStoreManager manager) {
      this.creationTime = System.currentTimeMillis();
      this.provider = provider;
      this.renewalInterval = (long) (manager.tokenRenewIntervalFactor * provider.getRenewalIntervalMillis());
    }

    boolean hasExpired() {
      return renewalInterval > 0 && ((System.currentTimeMillis() - creationTime) > renewalInterval);
    }

  }

  public static class TokenRenewer implements Runnable {

    private TokenStoreManager manager;

    TokenRenewer(TokenStoreManager manager) {
      this.manager = manager;
    }

    @Override
    public void run() {
      boolean tokensChanged = false;
      for (String alias : manager.tokenProviders.keySet()) {
        if (manager.tokenProviders.get(alias).hasExpired()) {
          LOG.debug("Token for {} has expired, reacquiring", alias);
          try {
            manager.obtainAndRegisterToken(alias, manager.tokenProviders.get(alias).provider);
          } catch (Exception e) {
            LOG.error("Failed to reacquire token: {}", e.getMessage());
            throw new RuntimeException(e);
          }
          manager.tokenProviders.put(alias,
              new ExpiringTokenProvider(manager.tokenProviders.get(alias).provider, manager));
          tokensChanged = true;
        }
      }
      if (tokensChanged) {
        manager.writeTokenStore();
      }
    }

  }

  public void start() {
    if (tokenProviders.isEmpty()) {
      LOG.info("No TokenProviders specified, not starting token renewal thread");
    } else {
      if (!isStarted.get()) {
        writeTokenStore();
        executorService.scheduleAtFixedRate(new TokenRenewer(this),
            providerCheckIntervalMillis, providerCheckIntervalMillis, TimeUnit.MILLISECONDS);
        isStarted.set(true);
        LOG.info("Started token renewal thread");
      }
    }
  }

  public void stop() {
    if (!tokenProviders.isEmpty()) {
      if (isStarted.get()) {
        executorService.shutdownNow();
      }
      isStarted.set(false);
      cleanupTokenStoreFiles();
      LOG.info("Stopped token renewal thread");
    }
  }

}
