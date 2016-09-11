package com.cloudera.labs.envelope.input.stream.translate;

import java.lang.reflect.Constructor;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import scala.Tuple2;

public abstract class Translator<T> { 
    
    public static final String TYPE_CONFIG_NAME = "type";
    
    protected Config config;
    
    public Translator(Config config) {
        this.config = config;
    }
    
    public abstract Row translate(T key, T message) throws Exception;
    
    public Row translate(T message) throws Exception {
        return translate(null, message);
    }
    
    public abstract StructType getSchema();
    
    public static Translator<?> translatorFor(Config config) throws Exception {
        if (!config.hasPath(TYPE_CONFIG_NAME)) {
            throw new RuntimeException("Translator type not specified");
        }
        
        String translatorType = config.getString(TYPE_CONFIG_NAME);
        
        Translator<?> translator = null;
        
        if (translatorType.equals("kvp")) {
            translator = new KVPTranslator(config);
        }
        else if (translatorType.equals("delimited")) {
            translator = new DelimitedTranslator(config);
        }
        else if (translatorType.equals("avro")) {
            translator = new AvroTranslator(config);
        }
        else {
            Class<?> clazz = Class.forName(translatorType);
            Constructor<?> constructor = clazz.getConstructor(Config.class);
            translator = (Translator<?>)constructor.newInstance(config);
        }
        
        return translator;
    }
    
    @SuppressWarnings("serial")
    public static class TranslateFunction<T> implements Function<Tuple2<T, T>, Row> {
        private Config config;
        private Translator<T> translator;
        
        private static Logger LOG = LoggerFactory.getLogger(TranslateFunction.class);
        
        public TranslateFunction(Config config) {
            this.config = config;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Row call(Tuple2<T, T> keyAndMessage) throws Exception {
            T key = keyAndMessage._1;
            T message = keyAndMessage._2;
            
            if (translator == null) {
                translator = (Translator<T>)Translator.translatorFor(config);
                LOG.info("Translator created: " + translator.getClass().getName());
            }
            
            return translator.translate(key, message);
        }
    }
    
}
