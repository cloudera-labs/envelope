package com.cloudera.labs.envelope.translate;

import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 *
 */
public class VoidGenericRecordTranslator extends Translator<Void, GenericRecord> {
  public VoidGenericRecordTranslator(Properties props) {
    super(Void.class, GenericRecord.class, props);
  }

  /**
   * Translate the arriving keyed string message to a typed record.
   *
   * @param key     The string key of the arriving message.
   * @param message The arriving string message.
   * @return The translated Apache Avro record.
   */
  @Override
  public GenericRecord translate(Void key, GenericRecord message) throws Exception {
    return message;
  }

  /**
   * @return The Avro schema for the records that the translator generates.
   */
  @Override
  public Schema getSchema() {
    return null;
  }
}
