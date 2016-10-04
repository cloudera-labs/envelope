package com.cloudera.labs.envelope.translate;

import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 */
public class KVPTranslatorTest {

  @Test
  public void translate() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator", "kvp");
    props.setProperty("translator.kvp.delimiter.kvp", ":");
    props.setProperty("translator.kvp.delimiter.field", "=");
    props.setProperty("translator.kvp.field.names", "foo");
    props.setProperty("translator.kvp.field.types", "string");

    Translator<String, String> translator = Translator.translatorFor(String.class, String.class, props);

    GenericRecord output = translator.translate("foo=bar");
    assertEquals("bar", output.get("foo").toString());
  }

  @Test
  public void translateMulti() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator", "kvp");
    props.setProperty("translator.kvp.delimiter.kvp", ":");
    props.setProperty("translator.kvp.delimiter.field", "=");
    props.setProperty("translator.kvp.field.names", "foo,blaz");
    props.setProperty("translator.kvp.field.types", "string,int");

    Translator<String, String> translator = Translator.translatorFor(String.class, String.class, props);

    GenericRecord output = translator.translate("foo=bar:blaz=123");
    assertEquals("bar", output.get("foo").toString());
    assertEquals(123, output.get("blaz"));
  }

  @Test (expected = IllegalArgumentException.class)
  public void translateInvalidKeyClass() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator", "kvp");
    props.setProperty("translator.kvp.delimiter.kvp", ":");
    props.setProperty("translator.kvp.delimiter.field", "=");
    props.setProperty("translator.kvp.field.names", "foo");
    props.setProperty("translator.kvp.field.types", "string");

    Translator<Integer, String> translator = Translator.translatorFor(Integer.class, String.class, props);

    translator.translate("foo=bar");
  }

  @Test (expected = IllegalArgumentException.class)
  public void translateInvalidMessageClass() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator", "kvp");
    props.setProperty("translator.kvp.delimiter.kvp", ":");
    props.setProperty("translator.kvp.delimiter.field", "=");
    props.setProperty("translator.kvp.field.names", "foo");
    props.setProperty("translator.kvp.field.types", "string");

    Translator<String, Integer> translator = Translator.translatorFor(String.class, Integer.class, props);

    translator.translate(123);
  }

}