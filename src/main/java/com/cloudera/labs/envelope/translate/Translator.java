package com.cloudera.labs.envelope.translate;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for translators to extend.
 */
public abstract class Translator<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(Translator.class);

  private static final ThreadLocal<Map<String, Translator<?, ?>>> translators = new ThreadLocal<>();

  /**
   * The properties of the translator.
   */
  protected final Properties props;
  protected final Class<K> keyClass;
  protected final Class<V> messageClass;

  /**
   *
   * @param keyClass
   * @param messageClass
   * @param props
   */
  public Translator(Class<K> keyClass, Class<V> messageClass, Properties props) {
    this.props = props;
    this.keyClass = keyClass;
    this.messageClass = messageClass;
  }

  /**
   * Translate the arriving keyed string message to a typed record.
   *
   * @param key     The string key of the arriving message.
   * @param message The arriving string message.
   * @return The translated Apache Avro record.
   */
  public abstract GenericRecord translate(K key, V message) throws Exception;

  /**
   * Translate the arriving message to a typed record.
   *
   * @param message The arriving string message.
   * @return The translated Avro record.
   */
  public GenericRecord translate(V message) throws Exception {
    return translate(null, message);
  }

  /**
   * @return The Avro schema for the records that the translator generates.
   */
  public abstract Schema getSchema();

  /**
   * The translator for the application.
   *
   * @param props The properties for the application.
   * @return The translator.
   * @throws IllegalArgumentException If keyClass or messageClass are invalid for the instantiated Translator
   */
  @SuppressWarnings("unchecked") // Expressly checked for runtime classes alignment
  public static <K, V> Translator<K, V> translatorFor(Class<K> keyClass, Class<V> messageClass, Properties props) throws Exception {

    if (translators.get() == null) {
      translators.set(new HashMap<String, Translator<?, ?>>());
    }

    String translatorName = props.getProperty("translator");
    Boolean cacheBoolean = Boolean.valueOf(props.getProperty("translator.cache", "true"));

    Translator<?, ?> translator = translators.get().get(translatorName);

    if (translator == null || !cacheBoolean) {
      switch (translatorName) {
        case "kvp":
          translator = new KVPTranslator(props);
          break;
        case "delimited":
          translator = new DelimitedTranslator(props);
          break;
        case "avro":
          translator = new AvroTranslator(props);
          break;
        case "morphline":
          translator = new MorphlineTranslator<>(keyClass, messageClass, props);
          break;
        default:
          Class<?> clazz = Class.forName(translatorName);
          Constructor<?> constructor = clazz.getConstructor(Properties.class);
          translator = (Translator) constructor.newInstance(props);
          break;
      }

      if (keyClass != translator.keyClass || messageClass != translator.messageClass) {
        throw new IllegalArgumentException("Invalid key/message Class for Translator");
      }

      if (cacheBoolean) {
        translators.get().put(translatorName, translator);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Caching new Transformer[{}] for thread:{} [{}]", translatorName,
            Thread.currentThread().getName(), translator.getClass());
      }

      return (Translator<K, V>) translator;
    }

    if (keyClass != translator.keyClass || messageClass != translator.messageClass) {
      throw new IllegalArgumentException("Invalid key/message Class for Translator");
    }

    return (Translator<K, V>) translator;
  }

}
