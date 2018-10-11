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

package com.cloudera.labs.envelope.validate;

import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.net.URI;
import java.util.Set;

public class FilesystemPathAccessibleValidation implements Validation {
  
  private String path;

  public FilesystemPathAccessibleValidation(String path) {
    this.path = path;
  }

  @Override
  public ValidationResult validate(Config config) {
    if (!config.hasPath(path)) {
      return new ValidationResult(this, Validity.VALID, "Configuration '" + path +
          "' does not exist, so will not check if its value as a filesystem path could be opened");
    }
    
    String filesystemPath = config.getString(path);
    
    InputStream stream;
    try {
      FileSystem fs = FileSystem.get(new URI(filesystemPath), new Configuration());
      stream = fs.open(new Path(filesystemPath));
      stream.close();
    }
    catch (Exception e) {
      return new ValidationResult(this, Validity.INVALID,
          "Filesystem path '" + filesystemPath + "' could not be opened.", e);
    }
    
    return new ValidationResult(this, Validity.VALID,
        "Filesystem path '" + filesystemPath + "' could be opened");
  }
  
  @Override
  public String toString() {
    return "Filesystem path validation that configuration '" + path + "' must exist and be openable";
  }

  @Override
  public Set<String> getKnownPaths() {
    return Sets.newHashSet(path);
  }

}
