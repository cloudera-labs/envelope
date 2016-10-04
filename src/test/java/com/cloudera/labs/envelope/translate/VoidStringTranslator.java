package com.cloudera.labs.envelope.translate;

import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/**
 *
 */
public class VoidStringTranslator extends Translator<Void, String> {

  public VoidStringTranslator(Properties props) {
    super(Void.class, String.class, props);
  }

  /**
   * Translate the arriving keyed string message to a typed record.
   *
   * @param key     The string key of the arriving message.
   * @param message The arriving string message.
   * @return The translated Apache Avro record.
   */
  @Override
  public GenericRecord translate(Void key, String message) throws Exception {
    Schema schema = SchemaBuilder.record("test").fields().name("foo").type().optional().stringType().endRecord();
    return new GenericRecordBuilder(schema).set("foo", message).build();
  }

  /**
   * @return The Avro schema for the records that the translator generates.
   */
  @Override
  public Schema getSchema() {
    return null;
  }
}
