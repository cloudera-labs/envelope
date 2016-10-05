package com.cloudera.labs.envelope.translate;

import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 */
public class DelimitedTranslatorTest {

  @Test
  public void translate() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator", "delimited");
    props.setProperty("translator.cache", "false");
    props.setProperty("translator.delimited.delimiter", ":");
    props.setProperty("translator.delimited.field.names", "foo");
    props.setProperty("translator.delimited.field.types", "string");

    Translator<String, String> translator = Translator.translatorFor(String.class, String.class, props);

    GenericRecord output = translator.translate("blaz");
    assertEquals("blaz", output.get("foo").toString());
  }

  @Test
  public void translateMulti() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator", "delimited");
    props.setProperty("translator.cache", "false");
    props.setProperty("translator.delimited.delimiter", ":");
    props.setProperty("translator.delimited.field.names", "foo,bar");
    props.setProperty("translator.delimited.field.types", "string,int");

    Translator<String, String> translator = Translator.translatorFor(String.class, String.class, props);

    GenericRecord output = translator.translate("blaz:123");
    assertEquals("blaz", output.get("foo").toString());
    assertEquals(123, output.get("bar"));
  }

  @Test (expected = ArrayIndexOutOfBoundsException.class)
  public void translateIncorrectFieldCount() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator", "delimited");
    props.setProperty("translator.cache", "false");
    props.setProperty("translator.delimited.delimiter", ":");
    props.setProperty("translator.delimited.field.names", "foo");
    props.setProperty("translator.delimited.field.types", "string");

    Translator<String, String> translator = Translator.translatorFor(String.class, String.class, props);

    translator.translate("bar:blaz");
  }

  @Test (expected = NumberFormatException.class)
  public void translateIncorrectFieldType() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator", "delimited");
    props.setProperty("translator.cache", "false");
    props.setProperty("translator.delimited.delimiter", ":");
    props.setProperty("translator.delimited.field.names", "foo");
    props.setProperty("translator.delimited.field.types", "int");

    Translator<String, String> translator = Translator.translatorFor(String.class, String.class, props);

    translator.translate("bar");
  }

  @Test (expected = IllegalArgumentException.class)
  public void translateInvalidKeyClass() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator", "delimited");
    props.setProperty("translator.cache", "false");
    props.setProperty("translator.delimited.delimiter", ":");
    props.setProperty("translator.delimited.field.names", "foo");
    props.setProperty("translator.delimited.field.types", "string");

    Translator<byte[], String> translator = Translator.translatorFor(byte[].class, String.class, props);

    translator.translate("foo");
  }

  @Test (expected = IllegalArgumentException.class)
  public void translateInvalidMessageClass() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator", "delimited");
    props.setProperty("translator.cache", "false");
    props.setProperty("translator.delimited.delimiter", ":");
    props.setProperty("translator.delimited.field.names", "foo");
    props.setProperty("translator.delimited.field.types", "string");

    Translator<String, GenericRecord> translator = Translator.translatorFor(String.class, GenericRecord.class, props);

    Schema schema = SchemaBuilder.record("t").fields().name("foo").type().optional().stringType().endRecord();
    GenericRecord input = new GenericRecordBuilder(schema).set("foo", "bar").build();
    translator.translate(input);
  }
}