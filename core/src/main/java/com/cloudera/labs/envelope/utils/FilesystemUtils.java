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

package com.cloudera.labs.envelope.utils;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

public class FilesystemUtils {

  public static String filesystemPathContents(String path) throws IOException, URISyntaxException {
    String contents;
    FileSystem fs = FileSystem.get(new URI(path), new Configuration());

    try (InputStream stream = fs.open(new Path(path));
         InputStreamReader reader = new InputStreamReader(stream, Charsets.UTF_8)) {
      contents = CharStreams.toString(reader);
    }

    return contents;
  }

}
