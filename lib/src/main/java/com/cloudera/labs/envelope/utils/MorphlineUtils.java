/*
 * Copyright (c) 2015-2018, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.labs.envelope.utils;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Notifications;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MorphlineUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MorphlineUtils.class);
  private static final ThreadLocal<Map<String, Pipeline>> pipelineCache = new ThreadLocal<>();
  private static final String SEPARATOR = "#";

  static {
    pipelineCache.set(new HashMap<String, Pipeline>());
  }

  /**
   *
   * @param morphlineFile
   * @param morphlineId
   * @return
   */
  public static Pipeline getPipeline(String morphlineFile, String morphlineId) {
    LOG.debug("Retrieving Pipeline[{}#{}]", morphlineFile, morphlineId);

    if (null == pipelineCache.get()) {
      LOG.trace("Initializing ThreadLocal cache");
      pipelineCache.set(new HashMap<String, Pipeline>());
      return null;
    }

    return pipelineCache.get().get(morphlineFile + SEPARATOR + morphlineId);
  }

  /**
   *
   * @param morphlineFile
   * @param morphlineId
   * @param collector
   * @param isProduction
   * @return
   */
  public static Pipeline setPipeline(String morphlineFile, String morphlineId, Collector collector, boolean isProduction) {
    LOG.debug("Constructing Pipeline[{}#{}]", morphlineFile, morphlineId);

    // Set up the Morphline context and handler
    MorphlineContext context = new MorphlineContext.Builder()
        .setExceptionHandler(new FaultTolerance(isProduction, false))
        .build();

    // Compile the Morphline process
    Command morphline;
    try {
      morphline = new Compiler().compile(
          new File(morphlineFile),
          morphlineId,
          context,
          collector);
    } catch (Exception e) {
      throw new MorphlineCompilationException("Morphline compilation error", null, e);
    }

    // Create the pipeline wrapper
    Pipeline pipeline = new Pipeline(morphline, collector);

    // Ensure shutdown notification to Morphline commands esp in streaming environments
    JVMUtils.closeAtShutdown(pipeline);

    // Prep the pipeline
    Notifications.notifyBeginTransaction(pipeline.getMorphline());

    // Register the pipeline into the cache
    if (null == pipelineCache.get()) {
      pipelineCache.set(new HashMap<String, Pipeline>());
    }
    pipelineCache.get().put(morphlineFile + SEPARATOR + morphlineId, pipeline);

    LOG.trace("Pipeline[{}#{}] prepared", morphlineFile, morphlineId);
    return pipeline;
  }

  public static List<Record> executePipeline(Pipeline pipeline, Record inputRecord) {
    return executePipeline(pipeline, inputRecord, true);
  }

  public static List<Record> executePipeline(Pipeline pipeline, Record inputRecord, boolean errorOnEmpty) {
    Command morphline = pipeline.getMorphline();

    try {
      LOG.trace("Input Record: {}", inputRecord);

      // Process the Record
      Notifications.notifyStartSession(morphline);
      boolean success = morphline.process(inputRecord);
      Notifications.notifyCommitTransaction(morphline);

      if (!success) {
        throw new MorphlineRuntimeException("Morphline failed to process incoming Record: " + inputRecord);
      }

      // Collect the output
      List<Record> outputRecords = pipeline.getCollector().getRecords();
      if (errorOnEmpty && !outputRecords.iterator().hasNext()) {
        throw new MorphlineRuntimeException("Morphline did not produce output Record(s)");
      }
      LOG.trace("Output Record(s): {}", outputRecords);

      return outputRecords;

    } catch (RuntimeException e) {
      Notifications.notifyRollbackTransaction(morphline);
      // TODO : Review exception handling
      LOG.warn("Morphline failed to execute properly on incoming Record: " + inputRecord, e);
      throw e;
    }
  }

  @SuppressWarnings("serial")
  public static FlatMapFunction<Row, Row> morphlineMapper(final String morphlineFile, final String morphlineId,
                                                          final StructType outputSchema, final boolean errorOnEmpty) {
    return new FlatMapFunction<Row, Row>() {
      @Override
      public Iterator<Row> call(Row row) throws Exception {
        // Retrieve the Command pipeline via ThreadLocal
        Pipeline pipeline = MorphlineUtils.getPipeline(morphlineFile, morphlineId);

        if (null == pipeline) {
          pipeline = MorphlineUtils.setPipeline(morphlineFile, morphlineId, new Collector(), true);
        }

        // Convert each Row into a Record
        StructType inputSchema = row.schema();
        if (null == inputSchema) {
          throw new RuntimeException("Row does not have an associated StructType schema");
        }

        Record inputRecord = new Record();
        String[] fieldNames = inputSchema.fieldNames();

        // TODO : Confirm nested object conversion
        for (int i = 0; i < fieldNames.length; i++) {
          inputRecord.put(fieldNames[i], row.get(i));
        }

        // Process each Record via the Command pipeline
        List<Record> outputRecords = MorphlineUtils.executePipeline(pipeline, inputRecord, errorOnEmpty);

        // Convert each Record into a new Row
        List<Row> outputRows = Lists.newArrayListWithCapacity(outputRecords.size());
        for (Record record : outputRecords) {
          outputRows.add(MorphlineUtils.convertToRow(outputSchema, record));
        }

        return outputRows.iterator();
      }
    };
  }

  /**
   * <p>Converts a Morphline {@link Record} into a Spark DataFrame {@link Row}.  The first value of Record's field is
   * used; all other values for the given field are ignored.</p>
   * <p>Throws a RuntimeException on conversion error.</p>
   * @see Record#getFirstValue(String)
   * @param schema The target Row schema
   * @param record The source Record object
   * @return A Row object based on the target schema
   */
  public static Row convertToRow(StructType schema, Record record) {
    LOG.debug("Converting Record to Row: {}", record);

    List<Object> values = Lists.newArrayList();
    ListMultimap<String, Object> activeFields = record.getFields();

    for (StructField field : schema.fields()) {
      String fieldName = field.name();
      DataType fieldDataType = field.dataType();

      Object recordValue = null;
      if (activeFields.containsKey(fieldName)) {
        recordValue = record.getFirstValue(fieldName);
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Converting Field[{} => {}] to DataType[{}]]", fieldName, recordValue, fieldDataType);
      }

      if (null != recordValue) {
        try {
          Object result = RowUtils.toRowValue(recordValue, field.dataType());
          values.add(result);
        } catch (Exception e) {
          throw new RuntimeException(String.format("Error converting Field[%s => %s] to DataType[%s]", fieldName,
              recordValue, fieldDataType), e);
        }
      } else {
        if (field.nullable()) {
          LOG.trace("Setting Field[{} => null] for DataType[{}]", fieldName, fieldDataType);
          values.add(null);
        } else {
          throw new RuntimeException(String.format("Error converting Field[%s => null] for DataType[%s]: DataType " +
              "cannot contain 'null'", fieldName, fieldDataType));
        }
      }
    }

    Row result = RowFactory.create(values.toArray());
    LOG.trace("Converted Record to Row: {}", result);

    return result;
  }

  /**
   * A Morphline Command that collects Records produced by a pipeline of commands.
   */
  public static class Collector implements Command {
    private static final Logger LOG = LoggerFactory.getLogger(Collector.class);
    private static final int ESTIMATED_SIZE = 1;

    private final List<Record> collected;

    public Collector() {
      this.collected = new ArrayList<>(ESTIMATED_SIZE);
    }

    public List<Record> getRecords() {
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

    // TODO : Set parent Command
    @Override
    public Command getParent() {
      return null;
    }
  }

  /**
   *
   */
  public static class Pipeline implements Closeable {

    private Command morphline;
    private Collector collector;

    Pipeline(Command morphline, Collector collector) {
      this.morphline = morphline;
      this.collector = collector;
    }

    Command getMorphline() {
      return this.morphline;
    }

    Collector getCollector() {
      return this.collector;
    }

    @Override
    public void close() throws IOException {
      Notifications.notifyShutdown(this.morphline);
    }

  }
}
