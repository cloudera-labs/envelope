package com.cloudera.labs.envelope.translate;

import java.io.ByteArrayOutputStream;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 */
public class AvroTranslatorTest {

  @Test
  public void translate() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator", "avro");
    props.setProperty("translator.avro.field.names", "foo");
    props.setProperty("translator.avro.field.types", "string");

    Translator typedTranslator = Translator.translatorFor(props);

    Schema schema = SchemaBuilder.record("t").fields().name("foo").type().optional().stringType().endRecord();
    GenericRecord input = new GenericRecordBuilder(schema).set("foo", "YES?!").build();

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    datumWriter.write(input, encoder);
    encoder.flush();

    byte[] inputBytes = outputStream.toByteArray();

    GenericRecord output = typedTranslator.translate(inputBytes);
    assertEquals("YES?!", output.get("foo").toString());
  }

}