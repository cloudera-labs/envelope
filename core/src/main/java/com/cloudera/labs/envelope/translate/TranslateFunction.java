/*
 * Copyright (c) 2015-2019, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.labs.envelope.translate;

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.schema.DeclaresExpectingSchema;
import com.cloudera.labs.envelope.schema.DeclaresProvidingSchema;
import com.cloudera.labs.envelope.schema.UsesProvidedSchema;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

@SuppressWarnings("serial")
public class TranslateFunction implements FlatMapFunction<Row, Row>, InstantiatesComponents,
    DeclaresExpectingSchema, DeclaresProvidingSchema, UsesProvidedSchema {

  public static final String APPEND_RAW_ENABLED_CONFIG = "append.raw.enabled";
  public static final boolean APPEND_RAW_ENABLED_DEFAULT = false;
  public static final String HAD_ERROR_FIELD_NAME = "_had_error";

  private Config config;
  private StructType providedSchema;

  private transient Translator translator;

  private static Logger LOG = LoggerFactory.getLogger(TranslateFunction.class);

  public TranslateFunction(Config config) {
    this.config = config;
  }

  @Override
  public Iterator<Row> call(Row message) throws Exception {
    validateMessageSchema(message);
    Iterable<Row> translationResults;

    try {
      translationResults = getTranslator().translate(message);
    }
    catch (Exception e) {
      Row error = appendHadErrorFlag(message, true);
      return Collections.singleton(error).iterator();
    }

    List<Row> translated = Lists.newArrayList();
    for (Row translationResult : translationResults) {
      validateTranslatedSchema(translationResult);

      if (doesAppendRaw()) {
        translationResult = appendRawFields(translationResult, message);
      }
      translationResult = appendHadErrorFlag(translationResult, false);

      translated.add(translationResult);
    }

    return translated.iterator();
  }

  @Override
  public StructType getExpectingSchema() {
    return getTranslator().getExpectingSchema();
  }

  @Override
  public StructType getProvidingSchema() {
    StructType translatedSchema = getTranslator().getProvidingSchema();

    if (doesAppendRaw()) {
      translatedSchema = SchemaUtils.appendFields(
          translatedSchema, Arrays.asList(addFieldNameUnderscores(providedSchema).fields()));
    }

    return translatedSchema;
  }

  @Override
  public void receiveProvidedSchema(StructType providedSchema) {
    this.providedSchema = providedSchema;

    if (getTranslator() instanceof UsesProvidedSchema) {
      ((UsesProvidedSchema)getTranslator()).receiveProvidedSchema(providedSchema);
    }
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    return Collections.singleton(new InstantiatedComponent(
        getTranslator(configure), getTranslatorConfig(config), "Translator"
    ));
  }

  public Translator getTranslator() {
    return getTranslator(true);
  }

  public synchronized Translator getTranslator(boolean configure) {
    if (configure) {
      if (translator == null) {
        translator = TranslatorFactory.create(getTranslatorConfig(config), true);
        LOG.debug("Translator created: " + translator.getClass().getName());
      }

      return translator;
    }
    else {
      return TranslatorFactory.create(getTranslatorConfig(config), false);
    }
  }

  private Config getTranslatorConfig(Config config) {
    // Don't pass the append raw configuration to the translator itself as it doesn't use it
    return config.withoutPath(APPEND_RAW_ENABLED_CONFIG);
  }

  private void validateMessageSchema(Row message) {
    if (message.schema() == null) {
      throw new RuntimeException("Translator must be provided raw messages with an embedded schema");
    }

    if (!hasValueField(message)) {
      throw new RuntimeException("Translator must be provided raw messages with a '" +
          Translator.VALUE_FIELD_NAME + "' field");
    }
  }

  private void validateTranslatedSchema(Row translationResult) {
    if (translationResult.schema() == null) {
      throw new RuntimeException("Translator must translate to rows with an embedded schema");
    }
  }

  private boolean doesAppendRaw() {
    return ConfigUtils.getOrElse(config, APPEND_RAW_ENABLED_CONFIG, APPEND_RAW_ENABLED_DEFAULT);
  }

  private Row appendRawFields(Row translated, Row message) {
    for (StructField messageField : message.schema().fields()) {
      translated = RowUtils.append(
          translated,
          "_" + messageField.name(),
          messageField.dataType(),
          message.getAs(messageField.name()));
    }

    return translated;
  }

  private boolean hasValueField(Row message) {
    for (String fieldName : message.schema().fieldNames()) {
      if (fieldName.equals(Translator.VALUE_FIELD_NAME)) {
        return true;
      }
    }

    return false;
  }

  private StructType addFieldNameUnderscores(StructType without) {
    List<StructField> withFields = Lists.newArrayList();

    for (StructField withoutField : without.fields()) {
      String withName = "_" + withoutField.name();
      if (Arrays.asList(without.fieldNames()).contains(withName)) {
        throw new RuntimeException("Can not append raw field '" + withName + "' because that " +
            "field already exists as a result of the translation");
      }

      StructField withField = DataTypes.createStructField(
          withName, withoutField.dataType(), withoutField.nullable(), withoutField.metadata());

      withFields.add(withField);
    }

    return DataTypes.createStructType(withFields);
  }

  private Row appendHadErrorFlag(Row row, boolean hadError) {
    return RowUtils.append(row, HAD_ERROR_FIELD_NAME, DataTypes.BooleanType, hadError);
  }

}
