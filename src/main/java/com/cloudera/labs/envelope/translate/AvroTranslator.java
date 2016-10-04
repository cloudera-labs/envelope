package com.cloudera.labs.envelope.translate;

import com.cloudera.labs.envelope.utils.PropertiesUtils;
import com.cloudera.labs.envelope.utils.RecordUtils;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

/**
 * A translator implementation for binary Apache Avro generic record messages.
 */
public class AvroTranslator extends Translator<byte[], byte[]> {

  private Schema schema;

  public AvroTranslator(Properties props) {
    super(byte[].class, byte[].class, props);

    List<String> fieldNames = PropertiesUtils.propertyAsList(props, "translator.avro.field.names");
    List<String> fieldTypes = PropertiesUtils.propertyAsList(props, "translator.avro.field.types");
    schema = RecordUtils.schemaFor(fieldNames, fieldTypes);
  }

  @Override
  public GenericRecord translate(byte[] message) throws Exception {
    return translate(null, message);
  }

  /**
   * TODO Had some errors reading when the source schema was even slightly different than the specified read one
   *
   * @param key
   * @param message
   * @return
   * @throws Exception
   */
  @Override
  public GenericRecord translate(byte[] key, byte[] message) throws Exception {
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(getSchema());
    Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
    return reader.read(null, decoder);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

}
