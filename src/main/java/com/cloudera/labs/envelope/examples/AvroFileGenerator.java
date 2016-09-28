package com.cloudera.labs.envelope.examples;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 */
public class AvroFileGenerator {

  private static Schema schema = SchemaBuilder.record("AvroFileGenerator")
      .fields()
      .name("foo").type().stringType().noDefault()
      .name("bar").type().intType().noDefault()
      .endRecord();

  public static void main(final String[] args) throws Exception {

    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer);

    Path file = new Path(args[1]);
    Configuration conf = new Configuration();
    FileSystem fileSystem = FileSystem.newInstance(conf);

    fileWriter.create(schema, fileSystem.create(file, true));

    int iter = Integer.valueOf(args[0]);
    int count = 1;

    for (int i = 0; i < iter; i++) {
      GenericRecord r = new GenericRecordBuilder(schema)
          .set("foo", "Entry")
          .set("bar", count)
          .build();
      fileWriter.append(r);
      count++;
    }

    fileWriter.close();

  }
}
