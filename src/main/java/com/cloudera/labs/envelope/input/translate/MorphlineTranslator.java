package com.cloudera.labs.envelope.input.translate;

import com.cloudera.labs.envelope.utils.JVMUtils;
import com.cloudera.labs.envelope.utils.MorphlineUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
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
 * Morphline
 */
public class MorphlineTranslator<T> implements Translator<T>, Closeable {

  public static final String ENCODING_KEY = "encoding.key";

  // TODO : Move default parameter values to reference.conf? How? Each Translator is separate/nested?
  // This most likely should use Config.withFallback() on a per instance basis.
  public static final String ENCODING_MSG = "encoding.message";
  public static final String MORPHLINE = "morphline";
  public static final String MORPHLINE_ID = "morphline.id";
  public static final String PRODUCTION_MODE = "production.mode";
  public static final String FIELD_NAMES = "field.names";
  public static final String FIELD_TYPES = "field.types";

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineTranslator.class);
  private static final String TRANSLATOR_KEY = "_attachment_key";
  private static final String TRANSLATOR_KEY_CHARSET = "_attachment_key_charset";
  private String keyEncoding;
  private String messageEncoding;
  private StructType schema;
  private Command morphline;
  private MorphlineContext morphlineContext;
  private RecordCollector collector;

  @Override
  public void configure(Config config) {
    LOG.info("Preparing Morphline Translator");

    // Define the encoding values, if necessary
    this.keyEncoding = config.getString(ENCODING_KEY);
    this.messageEncoding = config.getString(ENCODING_MSG);

    // Set up the Morphline configuration, the file must be located on the local file system
    String morphlineFile = config.getString(MORPHLINE);
    String morphlineId = config.getString(MORPHLINE_ID);

    if (morphlineFile == null || morphlineFile.trim().length() == 0) {
      throw new MorphlineCompilationException("Missing or empty Morphline File configuration parameter", null);
    }

    // Construct the StructType schema for the Rows
    List<String> fieldNames = config.getStringList(FIELD_NAMES);
    List<String> fieldTypes = config.getStringList(FIELD_TYPES);
    this.schema = RowUtils.structTypeFor(fieldNames, fieldTypes);

    // Set up the Morphline Context and handler
    boolean isProduction = config.getBoolean(PRODUCTION_MODE);
    this.morphlineContext = new MorphlineContext.Builder()
        .setExceptionHandler(new FaultTolerance(isProduction, false))
        .build();

    // Initialize the final Command in the Morphline process
    this.collector = new RecordCollector();

    // Compile the Morphline process
    try {
      this.morphline = new Compiler().compile(
          new File(morphlineFile),
          morphlineId,
          this.morphlineContext,
          this.collector);
    } catch (Exception e) {
      throw new MorphlineCompilationException("Morphline compilation error", null, e);
    }

    // Ensure shutdown notification to Morphline commands
    JVMUtils.closeAtShutdown(this);

    Notifications.notifyBeginTransaction(this.morphline);
    LOG.info("Done preparing Morphline Translator");
  }

  @Override
  public StructType getSchema() {
    return this.schema;
  }

  @Override
  public Iterable<Row> translate(T key, T message) throws Exception {
    Record inputRecord = new Record();
    List<Record> outputRecords;

    // Set up the message as _attachment_body (standard Morphline convention)
    if (message instanceof String) {
      inputRecord.put(Fields.ATTACHMENT_BODY, ((String) message).getBytes(this.messageEncoding));
    } else {
      inputRecord.put(Fields.ATTACHMENT_BODY, message);
    }
    inputRecord.put(Fields.ATTACHMENT_CHARSET, this.messageEncoding);

    // Add the key as a custom Record field
    if (null != key) {
      inputRecord.put(TRANSLATOR_KEY, key);
      inputRecord.put(TRANSLATOR_KEY_CHARSET, this.keyEncoding);
    }

    try {
      LOG.trace("Input Record: {}", inputRecord);

      // Process the Record
      Notifications.notifyStartSession(this.morphline);
      boolean success = this.morphline.process(inputRecord);
      Notifications.notifyCommitTransaction(this.morphline);

      if (!success) {
        throw new MorphlineRuntimeException("Morphline failed to process incoming Record: " + inputRecord);
      }

      // Collect the output
      outputRecords = this.collector.getRecords();
      if (!outputRecords.iterator().hasNext()) {
        throw new MorphlineRuntimeException("Morphline did not produce output Record(s)");
      }
      LOG.trace("Output Record(s): {}", outputRecords);

      // Convert output to Rows
      List<Row> outputRows = Lists.newArrayListWithCapacity(outputRecords.size());
      for (Record record: outputRecords) {
        outputRows.add(MorphlineUtils.convertToRow(this.schema, record));
      }
      return outputRows;

    } catch (RuntimeException e) {
      Notifications.notifyRollbackTransaction(this.morphline);
      // TODO : Review exception handling
      LOG.warn("Morphline failed to execute properly on incoming Record: " + inputRecord, e);
      this.morphlineContext.getExceptionHandler().handleException(e, inputRecord);
    }

    // Fail fast if the ExceptionHandler doesn't throw an error
    throw new RuntimeException("Morphline failed to produce valid output");
  }

  @Override
  public void close() throws IOException {
    Notifications.notifyShutdown(morphline);
  }

  protected class RecordCollector implements Command {
    private List<Record> collected = Lists.newArrayListWithExpectedSize(1);

    List<Record> getRecords() {
      return this.collected;
    }

    private void reset() {
      LOG.trace("Resetting collector");
      this.collected.clear();
    }

    @Override
    public void notify(Record notification) {
      for (Object event : Notifications.getLifecycleEvents(notification)) {
        if (event == Notifications.LifecycleEvent.START_SESSION) {
          reset();
        }
      }
    }

    @Override
    public boolean process(Record record) {
      this.collected.add(record);
      return true;
    }

    @Override
    public Command getParent() {
      return null;
    }
  }
}
