package com.cloudera.labs.envelope.translate;

import com.cloudera.labs.envelope.utils.JVMUtils;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MorphlineTranslator extends Translator implements Closeable {

  private static final String DEFAULT_ENCODING = "UTF-8";
  private static final String TRANSLATOR_KEY = "_attachment_key";
  private static final String TRANSLATOR_KEY_CHARSET = "_attachment_key_charset";
  private static final String TRANSLATOR_SCHEMA = "_translator_schema";

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineTranslator.class);

  private Charset keyEncoding;
  private Charset messageEncoding;
  private Schema schema;
  private Command morphline;
  private MorphlineContext morphlineContext;
  private GenericRecordCollector collector;

  MorphlineTranslator(Properties props) {
    super(props);

    LOG.debug("Preparing morphline");

    try {
      this.keyEncoding = Charset.forName(props.getProperty("translator.morphline.encoding.key", DEFAULT_ENCODING));
    } catch (Exception e) {
      throw new MorphlineCompilationException("Invalid parameter: translator.morphline.encoding.key", null, e);
    }

    try {
      this.messageEncoding = Charset.forName(props.getProperty("translator.morphline.encoding.message",
          DEFAULT_ENCODING));
    } catch (Exception e) {
      throw new MorphlineCompilationException("Invalid parameter: translator.morphline.encoding.message", null, e);
    }

    String schemaFile = props.getProperty("translator.morphline.schema.file");
    if (schemaFile == null || schemaFile.trim().length() == 0) {
      throw new MorphlineCompilationException("Missing parameter: translator.morphline.schema.file", null);
    }

    try {
      this.schema = new Schema.Parser().parse(new File(schemaFile));
    } catch (Exception e) {
      throw new MorphlineCompilationException("Parse error: translator.morphline.schema.file", null, e);
    }

    String morphlineFile = props.getProperty("translator.morphline.file");
    String morphlineId = props.getProperty("translator.morphline.identifier");
    if (morphlineFile == null || morphlineFile.trim().length() == 0) {
      throw new MorphlineCompilationException("Missing parameter: translator.morphline.file", null);
    }

    String isProduction = props.getProperty("translator.morphline.production.mode", "true");

    this.morphlineContext = new MorphlineContext.Builder()
        .setExceptionHandler(new FaultTolerance(Boolean.valueOf(isProduction), false))
        .build();

    this.collector = new GenericRecordCollector();

    try {
      this.morphline = new Compiler().compile(
          new File(morphlineFile),
          morphlineId,
          morphlineContext,
          this.collector);
    } catch (Exception e) {
      throw new MorphlineCompilationException("Morphline compilation error", null, e);
    }

    // Ensure shutdown notification to Morphline commands
    JVMUtils.closeAtShutdown(this);

    Notifications.notifyBeginTransaction(morphline);
    LOG.info("Morphline ready");
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public GenericRecord translate(String key, String message) throws Exception {
    return translate(key.getBytes(this.keyEncoding), message.getBytes(this.messageEncoding));
  }

  @Override
  public GenericRecord translate(byte[] key, byte[] message) throws Exception {
    GenericRecord output = null;

    Record record = new Record();

    record.put(Fields.ATTACHMENT_BODY, message);
    record.put(Fields.ATTACHMENT_CHARSET, this.messageEncoding);
    record.put(TRANSLATOR_KEY, key);
    record.put(TRANSLATOR_KEY_CHARSET, this.keyEncoding);

    try {
      // Add required elements for toAvro command
      record.put(TRANSLATOR_SCHEMA, this.schema);

      LOG.debug("Processing record: {}", record);

      Notifications.notifyStartSession(morphline);
      boolean success = morphline.process(record);
      Notifications.notifyCommitTransaction(morphline);

      if (!success) {
        throw new MorphlineRuntimeException("Morphline failed to process record: " + record);
      }

      if (collector.getGenericRecord() != null) {
        output = collector.getGenericRecord();
      } else {
        throw new MorphlineRuntimeException("Morphline did not produce attachment");
      }

    } catch (RuntimeException e) {
      Notifications.notifyRollbackTransaction(morphline);
      this.morphlineContext.getExceptionHandler().handleException(e, record);
      LOG.warn("Morphline creating empty GenericRecord");
    }

    return (output != null) ? output : new GenericData.Record(this.schema);
  }

  @Override
  public void close() throws IOException {
    Notifications.notifyShutdown(morphline);
  }

  private class GenericRecordCollector implements Command {

    private GenericRecord collected;

    GenericRecordCollector() {
      reset();
    }

    GenericRecord getGenericRecord() {
      return this.collected;
    }

    void reset() {
      LOG.trace("Resetting collector");
      this.collected = null;
    }

    @Override
    public void notify(Record notification) {
      for (Object event : Notifications.getLifecycleEvents(notification)) {
        if (event == Notifications.LifecycleEvent.START_SESSION) {
          reset();
        }
        else if (event == Notifications.LifecycleEvent.SHUTDOWN) {
          reset();
        }
      }
    }

    @Override
    public boolean process(Record record) {
      int size = record.get(Fields.ATTACHMENT_BODY).size();

      if (size == 0) {
        throw new MorphlineRuntimeException("Morphline record did not return any attachments; Record: " + record);
      } else if (size > 1) {
        LOG.warn("Morphline record returned {} attachments; selecting first value only", size);
      }

      Object attachment = record.getFirstValue(Fields.ATTACHMENT_BODY);

      try {
        LOG.debug("Morphline results: {}", attachment);
        this.collected = (GenericRecord) attachment;
      } catch (ClassCastException cce) {
        throw new MorphlineRuntimeException("Invalid cast of attachment to GenericRecord; Record: " + record, cce);
      }
      return true;
    }

    @Override
    public Command getParent() {
      return null;
    }
  }
}
