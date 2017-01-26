package com.cloudera.labs.envelope.input.translate;

import java.lang.reflect.Constructor;

import com.typesafe.config.Config;

public class TranslatorFactory {
    
    public static final String TYPE_CONFIG_NAME = "type";

    public static Translator<?> create(Config config) throws Exception {
        if (!config.hasPath(TYPE_CONFIG_NAME)) {
            throw new RuntimeException("Translator type not specified");
        }
        
        String translatorType = config.getString(TYPE_CONFIG_NAME);
        
        Translator<?> translator = null;
        
        if (translatorType.equals("kvp")) {
            translator = new KVPTranslator();
        }
        else if (translatorType.equals("delimited")) {
            translator = new DelimitedTranslator();
        }
        else if (translatorType.equals("avro")) {
            translator = new AvroTranslator();
        }
        else if (translatorType.equals("morphline")) {
          translator = new MorphlineTranslator<>();
        }
        else {
            Class<?> clazz = Class.forName(translatorType);
            Constructor<?> constructor = clazz.getConstructor();
            translator = (Translator<?>)constructor.newInstance();
        }
        
        translator.configure(config);
        
        return translator;
    }
    
}
