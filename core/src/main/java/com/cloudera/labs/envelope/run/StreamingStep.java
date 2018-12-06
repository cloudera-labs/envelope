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

package com.cloudera.labs.envelope.run;

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.input.CanRecordProgress;
import com.cloudera.labs.envelope.input.StreamInput;
import com.cloudera.labs.envelope.schema.InputTranslatorCompatibilityValidation;
import com.cloudera.labs.envelope.schema.SchemaNegotiator;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.translate.TranslateFunction;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.Set;

/**
 * A streaming step is a data step that provides a DataFrame per Spark Streaming micro-batch.
 */
public class StreamingStep extends DataStep implements CanRecordProgress, ProvidesValidations, InstantiatesComponents {

  public static final String TRANSLATOR_WITHIN_INPUT_PROPERTY = "translator";
  public static final String TRANSLATOR_PROPERTY = DataStep.INPUT_TYPE + "." + TRANSLATOR_WITHIN_INPUT_PROPERTY;
  public static final String REPARTITION_NUM_PARTITIONS_PROPERTY = "input.repartition.partitions";

  private TranslateFunction translateFunction;

  public StreamingStep(String name) {
    super(name);
  }

  @Override
  public void configure(Config config) {
    super.configure(config);
  }

  @SuppressWarnings("rawtypes")
  public JavaDStream<?> getStream() throws Exception {
    JavaDStream stream = ((StreamInput)getInput(true)).getDStream();

    if (doesRepartition()) {
      stream = repartition(stream);
    }

    return stream;
  }

  @Override
  public void recordProgress(JavaRDD<?> batch) throws Exception {
    if (getInput(true) instanceof CanRecordProgress) {
      ((CanRecordProgress)getInput(true)).recordProgress(batch);
    }
  }

  @SuppressWarnings("unchecked")
  public Dataset<Row> translate(JavaRDD raw) {
    StreamInput streamInput = (StreamInput)getInput(true);
    TranslateFunction translateFunction = getTranslateFunction(config, true);

    // Encode the raw messages as rows (i.e. the raw value plus associated metadata fields)
    Dataset<Row> encoded = Contexts.getSparkSession().createDataFrame(
        raw.map(streamInput.getMessageEncoderFunction()), streamInput.getProvidingSchema());

    // Translate the raw message rows to structured rows
    return encoded.flatMap(translateFunction, RowEncoder.apply(translateFunction.getProvidingSchema()));
  }

  @Override
  public Step copy() {
    StreamingStep copy = new StreamingStep(name);
    copy.configure(config);
    
    copy.setSubmitted(hasSubmitted());
    
    if (hasSubmitted()) {
      copy.setData(getData());
    }
    
    return copy;
  }

  private boolean doesRepartition() {
    return config.hasPath(REPARTITION_NUM_PARTITIONS_PROPERTY);
  }

  private JavaDStream<?> repartition(JavaDStream<?> stream) {
    int numPartitions = config.getInt(REPARTITION_NUM_PARTITIONS_PROPERTY);

    return stream.repartition(numPartitions);
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(TRANSLATOR_PROPERTY, ConfigValueType.OBJECT)
        .optionalPath(REPARTITION_NUM_PARTITIONS_PROPERTY, ConfigValueType.NUMBER)
        .add(new InputTranslatorCompatibilityValidation())
        .handlesOwnValidationPath(TRANSLATOR_PROPERTY)
        .addAll(super.getValidations())
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    Set<InstantiatedComponent> components = Sets.newHashSet();

    components.addAll(super.getComponents(config, configure));

    if (config.hasPath(TRANSLATOR_PROPERTY)) {
      TranslateFunction translateFunction = getTranslateFunction(config, configure);
      components.addAll(translateFunction.getComponents(config.getConfig(TRANSLATOR_PROPERTY), configure));
    }

    return components;
  }

  private TranslateFunction getTranslateFunction(Config config, boolean configure) {
    if (configure) {
      if (translateFunction == null) {
        StreamInput streamInput = (StreamInput)getInput(true);
        translateFunction = new TranslateFunction(config.getConfig(TRANSLATOR_PROPERTY));

        SchemaNegotiator.negotiate(streamInput, translateFunction);
      }

      return translateFunction;
    }
    else {
      return new TranslateFunction(config.getConfig(TRANSLATOR_PROPERTY));
    }
  }

}
