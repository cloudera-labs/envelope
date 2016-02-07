package com.cloudera.fce.envelope.translator;

import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import com.cloudera.fce.envelope.RecordModel;
import com.cloudera.fce.envelope.utils.PropertiesUtils;
import com.cloudera.fce.envelope.utils.RecordUtils;

public class AvroTranslator extends Translator {
    
    private List<String> fieldNames;
    private List<String> fieldTypes;
    private RecordModel recordModel = new RecordModel();
    private Schema schema;
    
    public AvroTranslator(Properties props) {
        super(props);
        
        fieldNames = PropertiesUtils.propertyAsList(props, "translator.avro.field.names");
        fieldTypes = PropertiesUtils.propertyAsList(props, "translator.avro.field.types");
        schema = RecordUtils.schemaFor(fieldNames, fieldTypes);
        recordModel.setKeyFieldNames(PropertiesUtils.propertyAsList(props, "translator.avro.key.field.names"));
    }
    
    @Override
    public GenericRecord translate(byte[] key, byte[] message) throws Exception {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(getSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
        GenericRecord record = reader.read(null, decoder);
        
        return record;
    }
    
    @Override
    public String acceptsType() {
        return "bytearray";
    }
    
    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public RecordModel getRecordModel() {
        return recordModel;
    }
    
}
