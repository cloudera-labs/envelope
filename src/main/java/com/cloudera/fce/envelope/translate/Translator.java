package com.cloudera.fce.envelope.translate;

import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public abstract class Translator { 
    
    protected Properties props;
    
    public Translator(Properties props) {
        this.props = props;
    }
    
    public GenericRecord translate(String key, String message) throws Exception {
        throw new RuntimeException("Translator does not accept string-encoded messages.");
    }
    public GenericRecord translate(String message) throws Exception {
        return translate(null, message);
    }
    
    public GenericRecord translate(byte[] key, byte[] message) throws Exception {
        throw new RuntimeException("Translator does not accept byte-array-encoded messages.");
    }
    public GenericRecord translate(byte[] message) throws Exception {
        return translate(null, message);
    }
    
    public abstract MessageEncoding acceptsType();
    
    public abstract Schema getSchema();
    
    public static Translator translatorFor(Properties props) throws Exception {
        Translator translator = null;
        
        String translatorName = props.getProperty("translator");
        
        if (translatorName.equals("kvp")) {
            translator = new KVPTranslator(props);
        }
        else if (translatorName.equals("delimited")) {
            translator = new DelimitedTranslator(props);
        }
        else if (translatorName.equals("avro")) {
            translator = new AvroTranslator(props);
        }
        else {
            Class<?> clazz = Class.forName(translatorName);
            Constructor<?> constructor = clazz.getConstructor(Properties.class);
            translator = (Translator)constructor.newInstance(props);
        }
        
        return translator;
    }
    
}
