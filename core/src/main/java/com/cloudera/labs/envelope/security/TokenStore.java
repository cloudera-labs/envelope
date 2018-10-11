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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;

/**
 * Wraps the Hadoop {@link Credentials} class to allow a list of token
 * aliases to be written alongside the credentials file.
 */
public class TokenStore {

  private Credentials credentials = new Credentials();
  private Set<String> tokenAliases = new HashSet<>();

  /**
   * Add a token to the underlying {@link Credentials} object using the supplied token alias.
   * @param alias
   * @param token
   */
  public void addToken(String alias, Token token) {
    credentials.addToken(new Text(alias), token);
    tokenAliases.add(alias);
  }

  /**
   * Retrieve the token from the {@link Credentials} object using the supplied token alias
   * @param alias
   * @return a {@link Token} or null if no such token
   */
  public Token getToken(String alias) {
    if (tokenAliases.contains(alias)) {
      return credentials.getToken(new Text(alias));
    } else {
      return null;
    }
  }

  /**
   * Get the list of token aliases stored in this {@link Credentials} object
   * @return a list of Strings
   */
  public Collection<String> getTokenAliases() {
    return tokenAliases;
  }

  /**
   * Write the token aliases and {@link Credentials} to a Hadoop FileSystem.
   * @param file path of the file to be written
   * @param conf a Hadoop {@link} Configuration for the Hadoop FileSystem
   * @throws IOException if file cannot be written
   */
  public void write(String file, Configuration conf) throws IOException {
    Path outFile = new Path(file);
    try (FSDataOutputStream fos = outFile.getFileSystem(conf).create(outFile)) {
      WritableUtils.writeStringArray(fos, tokenAliases.toArray(new String[]{}));
      credentials.writeTokenStorageToStream(fos);
    }
    outFile.getFileSystem(conf).setPermission(outFile,
        new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));
    // Write the ready file
    outFile.getFileSystem(conf).create(new Path(file + ".READY")).close();
  }

  /**
   * Read the token aliases and {@link Credentials} from a Hadoop FileSystem.
   * @param file path of the file to be read
   * @param conf a Hadoop {@link} Configuration for the Hadoop FileSystem
   * @throws IOException if file cannot be read
   */
  public void read(String file, Configuration conf) throws IOException {
    Path inFile = new Path(file);
    try (FSDataInputStream fis = inFile.getFileSystem(conf).open(inFile)) {
      tokenAliases.addAll(Arrays.asList(WritableUtils.readStringArray(fis)));
      credentials.readTokenStorageStream(fis);
    }
  }

}
