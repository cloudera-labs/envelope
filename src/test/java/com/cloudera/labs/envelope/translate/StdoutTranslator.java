package com.cloudera.labs.envelope.translate;

import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 *
 */
public class StdoutTranslator extends Translator<Object, Object> {

  public StdoutTranslator(Properties props) {
    super(props);
  }

  @Override
  public GenericRecord translate(Object message) throws Exception {
    System.out.println("TRANSLATE :: " + message);
    return null;
  }

  @Override
  public GenericRecord translate(Object key, Object message) throws Exception {
    System.out.println("TRANSLATE :: " + key + " / " + message);
    return null;
  }

  /**
   * @return The Avro schema for the records that the translator generates.
   */
  @Override
  public Schema getSchema() {
    System.out.println("TRANSLATE :: getSchema()");
    return null;
  }
}
