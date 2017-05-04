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
package com.cloudera.labs.envelope.output;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import parquet.avro.AvroParquetReader;
import scala.Tuple2;

/**
 *
 */
public class TestFileSystemOutput {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static String FS_DATA = "/filesystem/sample-fs.json";

  private Config config;
  private ArrayList<Tuple2<MutationType, Dataset<Row>>> plannedRows;
  private File results;

  @Before
  public void setup() throws IOException {
    results = temporaryFolder.newFolder();

    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put(FileSystemOutput.FORMAT_CONFIG_NAME, "parquet");
    paramMap.put(FileSystemOutput.PATH_CONFIG_NAME, results.getPath());
    config = ConfigFactory.parseMap(paramMap);

    plannedRows = new ArrayList<>();
    Dataset<Row> rowDataset = Contexts.getSparkSession().read().json(
        TestFileSystemOutput.class.getResource(FS_DATA).getPath());
    Tuple2<MutationType, Dataset<Row>> input = new Tuple2<>(MutationType.INSERT, rowDataset);
    plannedRows.add(input);
  }

  @After
  public void teardown() {
    plannedRows = null;
    config = null;
    results = null;
  }

  @Test (expected = RuntimeException.class)
  public void missingFormat() throws Exception {
    config = ConfigFactory.parseString(FileSystemOutput.FORMAT_CONFIG_NAME + ": null").withFallback(config);
    FileSystemOutput fileSystemOutput = new FileSystemOutput();
    fileSystemOutput.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void missingPath() throws Exception {
    config = ConfigFactory.parseString(FileSystemOutput.PATH_CONFIG_NAME + ": null").withFallback(config);
    FileSystemOutput fileSystemOutput = new FileSystemOutput();
    fileSystemOutput.configure(config);
  }

  @Test
  public void writeParquet() throws Exception {
    FileSystemOutput fileSystemOutput = new FileSystemOutput();
    fileSystemOutput.configure(config);
    fileSystemOutput.applyBulkMutations(plannedRows);

    File[] files = results.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("parquet");
      }
    });
    assertEquals("Incorrect number of Parquet files", 1, files.length);

    Path path = new Path(files[0].toURI());
    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(path);
    //AvroParquetReader.Builder<GenericRecord> reader = AvroParquetReader.builder(path);

    int i = 0;
    GenericRecord record = reader.read();
    while (null != record) {
      i++;
      record = reader.read();
    }
    assertEquals("Invalid record count", 4, i);
  }

  @Test (expected = RuntimeException.class)
  public void missingPartitions() throws Exception {
    config = ConfigFactory.parseString(FileSystemOutput.PARTITION_COLUMNS_CONFIG + ": []").withFallback(config);
    FileSystemOutput fileSystemOutput = new FileSystemOutput();
    fileSystemOutput.configure(config);
  }

  @Test
  public void repartitioningParquetColumns() throws Exception {
    config = ConfigFactory.parseString(FileSystemOutput.PARTITION_COLUMNS_CONFIG + ": [ field4, field3 ]").withFallback(config);

    FileSystemOutput fileSystemOutput = new FileSystemOutput();
    fileSystemOutput.configure(config);
    fileSystemOutput.applyBulkMutations(plannedRows);

    File[] rootPartitions = results.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory();
      }
    });
    assertEquals("Incorrect number of root partitions", 2, rootPartitions.length);

    File[] firstPartition = results.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.equals("field4=fleas");
      }
    });
    assertEquals("Invalid root partition", 1, firstPartition.length);
    assertTrue("Root partition is not a directory", firstPartition[0].isDirectory());

    File[] secondPartition = firstPartition[0].listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.equals("field3=true");
      }
    });
    assertEquals("Invalid nested partition", 1, secondPartition.length);
    assertTrue("Nested partition is not a directory", secondPartition[0].isDirectory());

    Path path = new Path(secondPartition[0].toURI());
    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(path);
    //AvroParquetReader.Builder<GenericRecord> reader = AvroParquetReader.builder(path);

    int i = 0;
    GenericRecord record = reader.read();
    GenericRecord other = record;
    while (null != other) {
      i++;
      other = reader.read();
    }
    assertEquals("Invalid partitioned record count", 1, i);
    assertEquals("Invalid record value", "three", record.get("field2"));
    assertNull("Invalid record value", record.get("field3"));
  }
}
