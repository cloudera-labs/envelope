package com.cloudera.labs.envelope.source;

import com.cloudera.labs.envelope.translate.StdoutTranslator;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class HdfsBulkSourceTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void rddForText() throws Exception {
    File file = buildTextSource(temporaryFolder, "UTF-8");

    Properties props = new Properties();

    props.setProperty("translator", StdoutTranslator.class.getName());

    props.setProperty("source", "hdfs");
    props.setProperty("source.hdfs.path", file.getAbsolutePath());
    props.setProperty("source.hdfs.type", "text");

    SparkConf conf = new SparkConf();
    conf.setAppName("rddFor: HDFS text")
        .setMaster("local[*]");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    BulkSource source = BulkSource.bulkSourceFor(props);
    JavaRDD<GenericRecord> output = source.rddFor(jsc, props);

    output.take(3);

    jsc.close();
  }

  @Test
  public void rddForAvro() throws Exception {
    File file = buildAvroSource(temporaryFolder);

    Properties props = new Properties();

    props.setProperty("translator", StdoutTranslator.class.getName());

    props.setProperty("source", "hdfs");
    props.setProperty("source.hdfs.path", file.getAbsolutePath());
    props.setProperty("source.hdfs.type", "avro");

    SparkConf conf = new SparkConf();
    conf.setAppName("rddFor: HDFS avro")
        .setMaster("local[*]");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    BulkSource source = BulkSource.bulkSourceFor(props);
    JavaRDD<GenericRecord> output = source.rddFor(jsc, props);

    output.take(3);

    jsc.close();
  }


  private File buildTextSource(TemporaryFolder folder, String charset) throws IOException {
    File temp = folder.newFile();

    PrintWriter writer = new PrintWriter(temp, charset);
    writer.print("One line");
    writer.println();
    writer.print("Another line");
    writer.println();
    writer.close();

    return temp;
  }

  private File buildAvroSource(TemporaryFolder folder) throws IOException {
    File temp = folder.newFile();

    Schema schema = getSchema();
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);

    GenericRecord recordOne = builder
        .set("first", "One")
        .set("second", 7)
        .build();

    GenericRecord recordTwo = builder
        .set("first", "Two")
        .set("second", 11)
        .build();

    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer).create(schema, temp);

    fileWriter.append(recordOne);
    fileWriter.append(recordTwo);
    fileWriter.close();

    return temp;
  }

  private Schema getSchema() {
    return SchemaBuilder.record("Test").fields()
        .name("first").type().stringType().noDefault()
        .name("second").type().intType().noDefault()
        .endRecord();
  }

}