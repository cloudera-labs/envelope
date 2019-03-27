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

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.schema.Schema;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.JsonToStruct;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.util.Collections;

public class JSONTranslator implements Translator, ProvidesAlias, ProvidesValidations {

  private StructType schema;
  private ExpressionEncoder encoder;
  private JsonToStruct converter;

  public static final String SCHEMA_CONFIG = "schema";

  @Override
  public void configure(Config config) {
    schema = ComponentFactory.create(Schema.class, config.getConfig(SCHEMA_CONFIG), true).getSchema();

    // This is wading into internal Spark APIs but it allows us to be fully consistent
    // with Spark because we are delegating it all of the parsing work
    encoder = RowEncoder.apply(schema).resolveAndBind((Seq)schema.toAttributes(), SimpleAnalyzer$.MODULE$);
    converter = new JsonToStruct(schema, new scala.collection.immutable.HashMap<String, String>(), null);
  }

  @Override
  public Iterable<Row> translate(Row message) {
    String json = message.getAs("value");
    InternalRow translatedInternal = (InternalRow)converter.nullSafeEval(functions.lit(json));
    Row translated = (Row)encoder.fromRow(translatedInternal);

    return Collections.singleton(translated);
  }

  @Override
  public StructType getExpectingSchema() {
    return SchemaUtils.stringValueSchema();
  }

  @Override
  public StructType getProvidingSchema() {
    return schema;
  }

  @Override
  public String getAlias() {
    return "json";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(SCHEMA_CONFIG, ConfigValueType.OBJECT)
        .handlesOwnValidationPath(SCHEMA_CONFIG)
        .build();
  }

}
