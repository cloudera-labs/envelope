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
package com.cloudera.labs.envelope.input;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 */
public class TestFileSystemInput {

  private static final String CSV_DATA = "/filesystem/sample-fs.txt";

  private Config config;

  @Test (expected = RuntimeException.class)
  public void missingFormat() throws Exception {
    config = ConfigFactory.parseString(FileSystemInput.FORMAT_CONFIG + ": null").withFallback(config);
    FileSystemInput fileSystemInput = new FileSystemInput();
    fileSystemInput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void missingPath() throws Exception {
    config = ConfigFactory.parseString(FileSystemInput.PATH_CONFIG + ": null").withFallback(config);
    FileSystemInput fileSystemInput = new FileSystemInput();
    fileSystemInput.configure(config);
  }

  @Test
  public void readCsvNoOptions() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    assertEquals(4, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("Four", first.getString(3));
  }

  @Test
  public void readCsvWithOptions() throws Exception {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemInput.FORMAT_CONFIG, "csv");
    paramMap.put(FileSystemInput.PATH_CONFIG, FileSystemInput.class.getResource(CSV_DATA).getPath());
    paramMap.put(FileSystemInput.CSV_HEADER_CONFIG, "true");
    config = ConfigFactory.parseMap(paramMap);

    FileSystemInput csvInput = new FileSystemInput();
    csvInput.configure(config);

    Dataset<Row> dataFrame = csvInput.read();
    assertEquals(3, dataFrame.count());

    Row first = dataFrame.first();
    assertEquals("four", first.getString(3));
  }

}