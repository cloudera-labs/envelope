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

import com.cloudera.labs.envelope.component.Component;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of utility methods for security implementations.
 */
public class SecurityUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtils.class);

  private static final Text TOKEN_TYPE = new Text("EnvelopeToken");
  private static final Text TOKEN_SERVICE = new Text("Envelope");

  public static final String SECURITY_PREFIX = "security";
  public static final String RENEW_INTERVAL = "renew-interval";
  public static final String TOKENS_FILE = "tokenstore";
  public static final String TOKENS_CHECK_INTERVAL = "check-interval";
  public static final String TOKENS_RENEW_FACTOR = "renew-factor";
  public static final String TOKENS_DELETION_DELAY = "deletion-delay";
  /**
   * Create a new generic token containing a security token. The Hadoop Token type is set to "EnvelopeToken" and given
   * an identifier of 0 as a byte array.
   * @param rawToken byte array for the security token
   * @return a generic Token
   */
  public static Token createToken(byte[] rawToken) {
    return createToken(rawToken, 0);
  }

  /**
   * Create a new generic token containing a security token. The Hadoop Token type is set to "EnvelopeToken" and given
   * an identifier of generation as a byte array.
   * @param rawToken byte array for the security token
   * @param generation integer version of the token
   * @return a generic Token
   */
  public static Token createToken(byte[] rawToken, int generation) {
    return new Token(getIdentifier(generation), rawToken, TOKEN_TYPE, TOKEN_SERVICE);
  }

  private static byte[] getIdentifier(int generation) {
    return ByteBuffer.allocate(4).putInt(generation).array();
  }

  public static String getTokenStoreFilePath(Config config, boolean onDriver) throws IOException {
    String tokenFilePrefix;
    if (config.hasPath(TOKENS_FILE)) {
      tokenFilePrefix = config.getString(TOKENS_FILE);
    } else {
      String userName = UserGroupInformation.getCurrentUser().getShortUserName();
      String appId;
      if (onDriver) {
        appId = Contexts.getSparkSession().sparkContext().applicationId();
      } else {
        appId = SparkEnv.get().conf().getAppId();
      }
      tokenFilePrefix = String.format("/user/%s/.sparkStaging/%s/envelope_tokens", userName, appId);
    }
    return tokenFilePrefix;
  }

  /**
   * Get a list of token store files for the job. The {@value TOKENS_FILE}
   * prefix is suffixed with "*" to perform a glob search.
   * @param config application section of Envelope job config
   * @param hadoopConf Hadoop Configuration for the FileSystem
   * @return list of {@link Path}s of matching files
   * @throws IOException if the FileSystem cannot be read
   */
  public static List<Path> getExistingTokenStoreFiles(Config config, Configuration hadoopConf, boolean onDriver) throws IOException {
    Path tokenFilePrefix = new Path(getTokenStoreFilePath(config, onDriver));
    FileSystem fs = FileSystem.get(hadoopConf);
    FileStatus[] matches = fs.listStatus(tokenFilePrefix.getParent(),
        new GlobFilter(tokenFilePrefix.getName() + "*"));
    List<Path> existingFiles = new ArrayList<>();
    for (FileStatus f : matches) {
      if (!f.getPath().getName().endsWith("READY")) {
        existingFiles.add(f.getPath());
      }
    }
    return existingFiles;
  }

  /**
   * Delete a list of specified files, but only if they were last modified more than delay milliseconds ago.
   * @param files list of files to delete
   * @param delay files that were modified less than delay milliseconds ago will not be deleted
   * @param hadoopConf a Hadoop {@link Configuration} object
   * @throws IOException if a file cannot be deleted
   */
  public static void deleteTokenStoreFiles(List<Path> files, long delay, Configuration hadoopConf) throws IOException {
    try {
      if (files.isEmpty()) {
        return;
      }
      long deletionTime = System.currentTimeMillis();
      FileSystem fs = FileSystem.get(hadoopConf);
      for (Path p : files) {
        if ((deletionTime - fs.getFileStatus(p).getModificationTime()) > delay) {
          LOG.debug("Deleting TokenStore file at {}", p.toString());
          fs.delete(p, false);
          Path readyFile = new Path(p.toString() + ".READY");
          if (fs.exists(readyFile)) {
            LOG.debug("Deleting TokenStore ready file at {}", readyFile.toString());
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Could not delete the temporary TokenStore files: {}", e.getMessage());
    }
  }

  public static Validations getValidations() {
    return Validations.builder()
        .optionalPath(SECURITY_PREFIX + "." + RENEW_INTERVAL)
        .optionalPath(SECURITY_PREFIX + "." + TOKENS_FILE, ConfigValueType.STRING)
        .optionalPath(SECURITY_PREFIX + "." + TOKENS_CHECK_INTERVAL)
        .optionalPath(SECURITY_PREFIX + "." + TOKENS_DELETION_DELAY)
        .optionalPath(SECURITY_PREFIX + "." + TOKENS_RENEW_FACTOR, ConfigValueType.NUMBER)
        .build();
  }

  public static Set<InstantiatedComponent> getAllSecureComponents(InstantiatedComponent component) throws Exception {
    Set<InstantiatedComponent> secureComponents = Sets.newHashSet();

    Component thisComponent = component.getComponent();

    if (thisComponent instanceof UsesDelegationTokens) {
      secureComponents.add(component);
    }

    if (thisComponent instanceof InstantiatesComponents) {
      Set<InstantiatedComponent> instantiatedComponents =
          ((InstantiatesComponents)thisComponent).getComponents(component.getConfig(), true);
      for (InstantiatedComponent instantiatedComponent : instantiatedComponents) {
        secureComponents.addAll(getAllSecureComponents(instantiatedComponent));
      }
    }

    return secureComponents;
  }
}
