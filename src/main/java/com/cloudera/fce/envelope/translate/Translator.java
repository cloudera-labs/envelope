package com.cloudera.fce.envelope.translate;

import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public abstract class Translator { 
    
    /**
     * The properties of the translator.
     */
    protected Properties props;
    
    public Translator(Properties props) {
        this.props = props;
    }
    
    /**
     * Translate the arriving keyed string message to a typed record.
     * @param key The string key of the arriving message.
     * @param message The arriving string message.
     * @return The translated Avro record. 
     */
    public GenericRecord translate(String key, String message) throws Exception {
        throw new RuntimeException("Translator does not accept string-encoded messages.");
    }
    
    /**
     * Translate the arriving message to a typed record.
     * @param message The arriving string message.
     * @return The translated Avro record.
     */
    public GenericRecord translate(String message) throws Exception {
        return translate(null, message);
    }
    
    /**
     * Translate the arriving keyed binary message to a typed record.
     * @param key The binary key of the arriving message.
     * @param message The arriving binary message.
     * @return The translated Avro record.
     */
    public GenericRecord translate(byte[] key, byte[] message) throws Exception {
        throw new RuntimeException("Translator does not accept byte-array-encoded messages.");
    }
    
    /**
     * Translate the arriving binary message to a typed record.
     * @param message The arriving binary message.
     * @return The translated Avro record.
     * @throws Exception
     */
    public GenericRecord translate(byte[] message) throws Exception {
        return translate(null, message);
    }
    
    /**
     * @return The Avro schema for the records that the translator generates.
     */
    public abstract Schema getSchema();
    
    /**
     * The translator for the application.
     * @param props The properties for the application.
     * @return The translator.
     */
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
