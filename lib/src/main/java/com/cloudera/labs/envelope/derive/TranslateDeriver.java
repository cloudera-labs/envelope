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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.component.CanReturnErroredData;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.schema.SchemaNegotiator;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.translate.TranslateFunction;
import com.cloudera.labs.envelope.translate.TranslationResults;
import com.cloudera.labs.envelope.translate.Translator;
import com.cloudera.labs.envelope.translate.TranslatorFactory;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TranslateDeriver implements Deriver, ProvidesAlias, ProvidesValidations,
    InstantiatesComponents, CanReturnErroredData {

  public static final String STEP_CONFIG = "step";
  public static final String FIELD_CONFIG = "field";
  public static final String TRANSLATOR_CONFIG = "translator";

  private String stepName;
  private String fieldName;
  private Config translatorConfig;
  private Dataset<Row> errored;

  @Override
  public void configure(Config config) {
    this.stepName = config.getString(STEP_CONFIG);
    this.fieldName = config.getString(FIELD_CONFIG);
    this.translatorConfig = config.getConfig(TRANSLATOR_CONFIG);
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) {
    Dataset<Row> step = getStep(dependencies, fieldName);

    StructType translatedSchema = getTranslatedSchema(
        step.schema(), getTranslateFunction(translatorConfig).getProvidingSchema());

    JavaRDD<Row> translation = step.javaRDD().flatMap(
        new DeriverTranslateFunction(fieldName, translatorConfig));

    TranslationResults translationResults = new TranslationResults(
        translation, translatedSchema, step.schema());

    errored = translationResults.getErrored();

    return translationResults.getTranslated();
  }

  private Dataset<Row> getStep(Map<String, Dataset<Row>> dependencies, String fieldName) {
    if (dependencies.containsKey(stepName)) {
      Dataset<Row> step = dependencies.get(stepName);

      if (Lists.newArrayList(step.schema().fieldNames()).contains(fieldName)) {
        return step;
      }
      else {
        throw new RuntimeException("Step '" + stepName + "' does not contain field '" + fieldName + "'");
      }
    }
    else {
      throw new RuntimeException("Step '" + stepName + "' must be a dependency of this step");
    }
  }

  private StructType getTranslatedSchema(StructType baseSchema, StructType translationSchema) {
    StructType toTranslateRemoved = SchemaUtils.subtractSchema(baseSchema, Lists.newArrayList(fieldName));

    translationSchema = SchemaUtils.appendFields(translationSchema, Lists.newArrayList(
        DataTypes.createStructField(TranslateFunction.HAD_ERROR_FIELD_NAME, DataTypes.BooleanType, false)));

    return SchemaUtils.appendFields(toTranslateRemoved, Lists.newArrayList(translationSchema.fields()));
  }

  private TranslateFunction getTranslateFunction(Config translatorConfig) {
    TranslateFunction translateFunction = new TranslateFunction(translatorConfig);
    SchemaNegotiator.negotiate(this, translateFunction);

    return translateFunction;
  }

  @Override
  public String getAlias() {
    return "translate";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(STEP_CONFIG, ConfigValueType.STRING)
        .mandatoryPath(FIELD_CONFIG, ConfigValueType.STRING)
        .mandatoryPath(TRANSLATOR_CONFIG, ConfigValueType.OBJECT)
        .handlesOwnValidationPath(TRANSLATOR_CONFIG)
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    return Sets.newHashSet(new InstantiatedComponent(
        TranslatorFactory.create(config.getConfig(TRANSLATOR_CONFIG), configure),
        config.getConfig(TRANSLATOR_CONFIG),
        "Translator"));
  }

  @Override
  public Dataset<Row> getErroredData() {
    return errored;
  }

  private static class DeriverTranslateFunction implements FlatMapFunction<Row, Row> {
    private String fieldName;
    private Config translatorConfig;
    private TranslateFunction translateFunction;
    private StructType valueSchema;
    private StructType hadErrorFlagSchema;

    DeriverTranslateFunction(String fieldName, Config translatorConfig) {
      this.fieldName = fieldName;
      this.translatorConfig = translatorConfig;
    }

    @Override
    public Iterator<Row> call(Row row) throws Exception {
      if (translateFunction == null) {
        translateFunction = new TranslateFunction(translatorConfig);
        valueSchema = DataTypes.createStructType(Lists.newArrayList(DataTypes.createStructField(
            Translator.VALUE_FIELD_NAME, row.schema().apply(fieldName).dataType(), true)));
        hadErrorFlagSchema = DataTypes.createStructType(Lists.newArrayList(DataTypes.createStructField(
            TranslateFunction.HAD_ERROR_FIELD_NAME, DataTypes.BooleanType, false)));
      }

      // Create the single value row to be sent to the translator
      Row message = new RowWithSchema(valueSchema, row.getAs(fieldName));

      // Translate the single value row
      List<Row> translatedRows = Lists.newArrayList(translateFunction.call(message));

      // If successfully translated, remove the field that was translated from the original row and
      // append the fields from the translation, including the had error flag. If the translation
      // created multiple rows then the original row will be copied multiple times for each of the
      // translation rows.
      // If not successfully translated, return the original row with the had error flag.
      List<Row> translationResults = Lists.newArrayList();
      for (Row translatedRow : translatedRows) {
        if (!hadError(translatedRow)) {
          Row derivedRow = RowUtils.remove(row, fieldName);
          derivedRow = RowUtils.append(derivedRow, translatedRow);

          translationResults.add(derivedRow);
        }
        else {
          Row erroredRow = RowUtils.append(row, RowUtils.subsetRow(translatedRow, hadErrorFlagSchema));
          translationResults.add(erroredRow);
        }
      }

      return translationResults.iterator();
    }

    private boolean hadError(Row row) {
      return row.getAs(TranslateFunction.HAD_ERROR_FIELD_NAME);
    }
  }

}
