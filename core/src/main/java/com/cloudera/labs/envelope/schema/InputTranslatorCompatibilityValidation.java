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

package com.cloudera.labs.envelope.schema;

import com.cloudera.labs.envelope.input.Input;
import com.cloudera.labs.envelope.input.InputFactory;
import com.cloudera.labs.envelope.run.DataStep;
import com.cloudera.labs.envelope.run.StreamingStep;
import com.cloudera.labs.envelope.translate.Translator;
import com.cloudera.labs.envelope.translate.TranslatorFactory;
import com.cloudera.labs.envelope.validate.Validation;
import com.cloudera.labs.envelope.validate.ValidationResult;
import com.cloudera.labs.envelope.validate.Validity;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.spark.sql.types.StructField;

import java.util.Set;

public class InputTranslatorCompatibilityValidation implements Validation {

  @Override
  public ValidationResult validate(Config config) {
    Input input;
    Translator translator;
    try {
      input = InputFactory.create(
          config.getConfig(DataStep.INPUT_TYPE), false);
      translator = TranslatorFactory.create(
          config.getConfig(StreamingStep.TRANSLATOR_PROPERTY), false);
    }
    catch (Exception e) {
      return new ValidationResult(this, Validity.VALID,
          "Could not instantiate input and/or translator, so will not check if they" +
              " are compatible.");
    }

    String inputClass = input.getClass().getSimpleName();
    String translatorClass = translator.getClass().getSimpleName();

    if (translator instanceof UsesProvidedSchema && !(input instanceof DeclaresProvidingSchema)) {
      return new ValidationResult(this, Validity.INVALID,
          inputClass + " is not compatible with " + translatorClass +
          " because " + translatorClass + " requires " + inputClass + " to declare the schema that" +
          " it provides, but " + inputClass + " does not do so.");
    }

    if (input instanceof DeclaresProvidingSchema) {
      for (StructField translatorExpectingField : translator.getExpectingSchema().fields()) {
        boolean expectedFieldFound = false;
        for (StructField inputProvidingField : ((DeclaresProvidingSchema) input).getProvidingSchema().fields()) {
          if (translatorExpectingField.name().equals(inputProvidingField.name()) &&
              translatorExpectingField.dataType().equals(inputProvidingField.dataType())) {
            expectedFieldFound = true;
          }
        }

        if (!expectedFieldFound) {
          return new ValidationResult(this, Validity.INVALID,
              inputClass + " is not compatible with " + translatorClass + " because " +
                  inputClass + " does not provide expected " + "field '" +
                  translatorExpectingField.name() + "' with data type '" +
                  translatorExpectingField.dataType() + "'");
        }
      }
    }

    return new ValidationResult(this, Validity.VALID, "Input and translator are compatible");
  }

  @Override
  public Set<String> getKnownPaths() {
    return Sets.newHashSet(DataStep.INPUT_TYPE, StreamingStep.TRANSLATOR_PROPERTY);
  }

}