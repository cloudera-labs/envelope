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

import com.cloudera.labs.envelope.component.Component;
import com.cloudera.labs.envelope.schema.DeclaresExpectingSchema;
import com.cloudera.labs.envelope.schema.DeclaresProvidingSchema;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;

/**
 * Translators deserialize raw messages into structured Spark SQL Rows.
 */
public interface Translator extends Component, DeclaresExpectingSchema, DeclaresProvidingSchema {

  /**
   * The name of the field on the raw message row that is to be translated.
   */
  String VALUE_FIELD_NAME = "value";

  /**
   * Translate the raw message into one or more structured rows.
   * @param message The raw message plus associated metadata, encoded as a Spark SQL Row.
   *                The 'value' field must always provide the raw message, but can be of
   *                any data type. Other fields are optional, e.g. 'key', 'timestamp'.
   * @return An iterable collection of Spark SQL Rows for the keyed value. If the keyed value
   * only translates to a single Spark SQL Row then it can be wrapped with {@link Collections#singleton}.
   */
  Iterable<Row> translate(Row message) throws Exception;

  /**
   * Get the expected schema of the raw message. These are the fields on the message that must
   * be provided in order to complete translation. All translators must at least provide a 'value'
   * field, however the data type of that and any other field is up to the translator.
   * @return The Spark SQL schema for the Rows that represent the raw message. The convenience
   * method {@link SchemaUtils#stringValueSchema()} can be used to generate an expecting schema of
   * a single string value field, or {@link SchemaUtils#binaryValueSchema()} for a single binary
   * value field.
   */
  @Override
  StructType getExpectingSchema();

  /**
   * Get the schema of the translated messages.
   * @return The Spark SQL schema for the Rows that the translator will provide.
   */
  @Override
  StructType getProvidingSchema();

}
