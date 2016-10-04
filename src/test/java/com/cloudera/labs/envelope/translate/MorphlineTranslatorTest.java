package com.cloudera.labs.envelope.translate;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;
import org.kitesdk.morphline.api.MorphlineCompilationException;

/**
 *
 */
public class MorphlineTranslatorTest {

  private static final String SCHEMA_FILE = "/morphline-translator.avro";
  private static final String MORPHLINE_FILE = "/morphline-translator.conf";
  private static final String INVALID_SCHEMA_FILE = "/morphline-invalid.avro";
  private static final String INVALID_MORPHLINE_FILE = "/morphline-invalid.conf";

  private String getResourcePath(String resource) {
    return MorphlineTranslatorTest.class.getResource(resource).getPath();
  }

  @Test
  public void getSchema() throws Exception {
    Schema schema = new Schema.Parser().parse(new File(getResourcePath(SCHEMA_FILE)));

    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", getResourcePath(MORPHLINE_FILE));

    Translator<Void, Void> translator = new MorphlineTranslator<>(Void.class, Void.class, props);

    assertEquals("Schemas not equal", schema, translator.getSchema());

    //System.out.println(translator.getSchema().toString(true));
  }

  @Test
  public void translateString() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", getResourcePath(MORPHLINE_FILE));
    props.setProperty("translator.morphline.identifier", "default");

    Translator<Void, String> translator = new MorphlineTranslator<>(Void.class, String.class, props);

    GenericRecord record = translator.translate(null, "blaz");
    assertEquals("Invalid field value", "blaz", record.get("foo"));
    assertEquals("Invalid field value", 123, record.get("bar"));

    record = translator.translate(null, "foobar");
    assertEquals("Invalid field value", "foobar", record.get("foo"));
    assertEquals("Invalid field value", 123, record.get("bar"));
  }

  @Test
  public void translateByte() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", getResourcePath(MORPHLINE_FILE));
    props.setProperty("translator.morphline.identifier", "default");

    Translator<byte[], byte[]> translator = new MorphlineTranslator<>(byte[].class, byte[].class, props);

    GenericRecord record = translator.translate("key1".getBytes(Charset.forName("UTF-8")),
        "blaz".getBytes(Charset.forName("UTF-8")));
    assertEquals("Invalid field value", "blaz", record.get("foo"));
    assertEquals("Invalid field value", 123, record.get("bar"));

    record = translator.translate("key2".getBytes(Charset.forName("UTF-8")),
        "foobar".getBytes(Charset.forName("UTF-8")));
    assertEquals("Invalid field value", "foobar", record.get("foo"));
    assertEquals("Invalid field value", 123, record.get("bar"));
  }

  @Test
  public void translateByteEncoding() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", getResourcePath(MORPHLINE_FILE));
    props.setProperty("translator.morphline.identifier", "encoding");
    props.setProperty("translator.morphline.encoding.key", "UTF-16");
    props.setProperty("translator.morphline.encoding.message", "UTF-16");

    Translator<byte[], byte[]> translator = new MorphlineTranslator<>(byte[].class, byte[].class, props);

    GenericRecord record = translator.translate("key1".getBytes(Charset.forName("UTF-16")),
        "blaz".getBytes(Charset.forName("UTF-16")));
    assertEquals("Invalid field value", "blaz", record.get("foo"));
    assertEquals("Invalid field value", 123, record.get("bar"));

    record = translator.translate("key2".getBytes(Charset.forName("UTF-16")),
        "foobar".getBytes(Charset.forName("UTF-16")));
    assertEquals("Invalid field value", "foobar", record.get("foo"));
    assertEquals("Invalid field value", 123, record.get("bar"));
  }

  @Test
  public void translateAvro() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", getResourcePath(MORPHLINE_FILE));
    props.setProperty("translator.morphline.identifier", "typed-avro");

    Translator<Void, GenericRecord> translator = new MorphlineTranslator<>(Void.class, GenericRecord.class, props);

    Schema schema = SchemaBuilder.record("test").fields().name("foo").type().stringType().noDefault().endRecord();
    GenericRecord input = new GenericRecordBuilder(schema).set("foo", "blaz").build();

    GenericRecord record = translator.translate(null, input);
    assertEquals("Invalid field value", "blaz", record.get("foo"));
    assertEquals("Invalid field value", 123, record.get("bar"));

    input = new GenericRecordBuilder(schema).set("foo", "foobar").build();

    record = translator.translate(null, input);
    assertEquals("Invalid field value", "foobar", record.get("foo"));
    assertEquals("Invalid field value", 123, record.get("bar"));
  }

  @Test
  public void translateFailedProcessing() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", getResourcePath(MORPHLINE_FILE));
    props.setProperty("translator.morphline.identifier", "failed-process");

    Translator<String, String> translator = new MorphlineTranslator<>(String.class, String.class, props);
    GenericRecord record = translator.translate("key", "message");

    assertNull("Invalid field value", record.get("foo"));
    assertNull("Invalid field value", record.get("bar"));
  }

  @Test
  public void translateNoRecordReturned() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", getResourcePath(MORPHLINE_FILE));
    props.setProperty("translator.morphline.identifier", "no-return");

    Translator<String, String> translator = new MorphlineTranslator<>(String.class, String.class, props);
    GenericRecord record = translator.translate("key", "message");

    assertNull("Invalid field value", record.get("foo"));
    assertNull("Invalid field value", record.get("bar"));
  }

  @Test
  public void translateNoAttachmentReturned() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", getResourcePath(MORPHLINE_FILE));
    props.setProperty("translator.morphline.identifier", "no-attachment");

    Translator<String, String> translator = new MorphlineTranslator<>(String.class, String.class, props);
    GenericRecord record = translator.translate("key", "message");

    assertNull("Invalid field value", record.get("foo"));
    assertNull("Invalid field value", record.get("bar"));
  }

  @Test
  public void translateMultipleAttachmentsReturned() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", getResourcePath(MORPHLINE_FILE));
    props.setProperty("translator.morphline.identifier", "multiple-attachments");

    Translator<String, String> translator = new MorphlineTranslator<>(String.class, String.class, props);
    GenericRecord record = translator.translate("key", "message");

    assertEquals("Invalid field value", "additional", record.get("foo"));
    assertEquals("Invalid field value", 123, record.get("bar"));
  }

  @Test
  public void translateIncorrectAttachmentTypeReturned() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", getResourcePath(MORPHLINE_FILE));
    props.setProperty("translator.morphline.identifier", "incorrect-attachment");

    Translator<String, String> translator = new MorphlineTranslator<>(String.class, String.class, props);
    GenericRecord record = translator.translate("key", "message");

    assertNull("Invalid field value", record.get("foo"));
    assertNull("Invalid field value", record.get("bar"));
  }

  @Test (expected = MorphlineCompilationException.class)
  public void invalidKeyEncoding() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.encoding.key", "INVALID_CHARSET");
    new MorphlineTranslator<>(String.class, String.class, props);
  }

  @Test (expected = MorphlineCompilationException.class)
  public void invalidMessageEncoding() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.encoding.message", "INVALID_CHARSET");
    new MorphlineTranslator<>(String.class, String.class, props);;
  }

  @Test (expected = MorphlineCompilationException.class)
  public void missingSchemaFile() throws Exception {
    Properties props = new Properties();
    new MorphlineTranslator<>(String.class, String.class, props);
  }

  @Test (expected = MorphlineCompilationException.class)
  public void notFoundSchemaFile() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", "nope.avro");
    new MorphlineTranslator<>(String.class, String.class, props);
  }

  @Test (expected = MorphlineCompilationException.class)
  public void invalidSchemaFile() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(INVALID_SCHEMA_FILE));
    new MorphlineTranslator<>(String.class, String.class, props);
  }

  @Test (expected = MorphlineCompilationException.class)
  public void missingMorphlineFile() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    new MorphlineTranslator<>(String.class, String.class, props);
  }

  @Test (expected = MorphlineCompilationException.class)
  public void trimmedMorphlineFile() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", "  ");
    new MorphlineTranslator<>(String.class, String.class, props);
  }

  @Test (expected = MorphlineCompilationException.class)
  public void invalidMorphlineFile() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", getResourcePath(INVALID_MORPHLINE_FILE));
    new MorphlineTranslator<>(String.class, String.class, props);
  }

  @Test (expected = MorphlineCompilationException.class)
  public void invalidCommand() throws Exception {
    Properties props = new Properties();
    props.setProperty("translator.morphline.schema.file", getResourcePath(SCHEMA_FILE));
    props.setProperty("translator.morphline.file", getResourcePath(MORPHLINE_FILE));
    props.setProperty("translator.morphline.identifier", "invalid-command");
    new MorphlineTranslator<>(String.class, String.class, props);
  }
}