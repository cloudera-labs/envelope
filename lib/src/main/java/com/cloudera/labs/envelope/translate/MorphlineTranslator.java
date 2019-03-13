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
import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.schema.SchemaFactory;
import com.cloudera.labs.envelope.utils.MorphlineUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class MorphlineTranslator implements Translator, ProvidesAlias, ProvidesValidations,
    InstantiatesComponents {

  public static final String ENCODING_KEY = "encoding.key";
  public static final String ENCODING_MSG = "encoding.message";
  public static final String MORPHLINE = "morphline.file";
  public static final String MORPHLINE_ID = "morphline.id";
  public static final String SCHEMA_CONFIG = "schema";

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineTranslator.class);
  private static final String TRANSLATOR_KEY = "_attachment_key";
  private static final String TRANSLATOR_KEY_CHARSET = "_attachment_key_charset";

  private String keyEncoding;
  private String messageEncoding;
  private String morphlineFile;
  private String morphlineId;
  private StructType schema;
  private MorphlineUtils.Pipeline pipeline;

  @Override
  public void configure(Config config) {
    LOG.debug("Configuring Morphline translator");

    // Define the encoding values, if necessary
    this.keyEncoding = config.getString(ENCODING_KEY);
    this.messageEncoding = config.getString(ENCODING_MSG);

    // Set up the Morphline configuration, the file must be located on the local file system
    this.morphlineFile = config.getString(MORPHLINE);
    this.morphlineId = config.getString(MORPHLINE_ID);

    // Construct the StructType schema for the Rows
    this.schema = SchemaFactory.create(config.getConfig(SCHEMA_CONFIG), true).getSchema();
  }

  @Override
  public StructType getExpectingSchema() {
    return DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField("key", DataTypes.BinaryType, true),
        DataTypes.createStructField(Translator.VALUE_FIELD_NAME, DataTypes.BinaryType, false)
    });
  }

  @Override
  public StructType getProvidingSchema() {
    return schema;
  }

  @Override
  public Iterable<Row> translate(Row message) throws Exception {
    Object key = message.getAs("key");
    Object value = message.getAs(Translator.VALUE_FIELD_NAME);

    // Get the Morphline Command pipeline
    if (null == this.pipeline) {
      this.pipeline = MorphlineUtils.getPipeline(this.morphlineFile, this.morphlineId);

      // If null, then instantiate the pipeline
      if (null == this.pipeline) {
        this.pipeline = MorphlineUtils.setPipeline(this.morphlineFile, this.morphlineId, new MorphlineUtils.Collector(),true);
      }
    }

    // Construct the input Record
    Record inputRecord = new Record();

    // Set up the message as _attachment_body (standard Morphline convention)
    inputRecord.put(Fields.ATTACHMENT_BODY, value);
    inputRecord.put(Fields.ATTACHMENT_CHARSET, this.messageEncoding);

    // Add the key as a custom Record field
    if (null != key) {
      inputRecord.put(TRANSLATOR_KEY, key);
      inputRecord.put(TRANSLATOR_KEY_CHARSET, this.keyEncoding);
    }

    // TODO : Consider using the MorphlineContext exception handler
    // Execute the pipeline (runtime errors are not caught)
    List<Record> outputRecords = MorphlineUtils.executePipeline(this.pipeline, inputRecord);

    // Convert output to Rows
    List<Row> outputRows = Lists.newArrayListWithCapacity(outputRecords.size());
    for (Record output: outputRecords) {
      Row outputRow = MorphlineUtils.convertToRow(this.schema, output);

      outputRows.add(outputRow);
    }

    return outputRows;
  }

  @Override
  public String getAlias() {
    return "morphline";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(ENCODING_KEY, ConfigValueType.STRING)
        .mandatoryPath(ENCODING_MSG, ConfigValueType.STRING)
        .mandatoryPath(MORPHLINE, ConfigValueType.STRING)
        .mandatoryPath(MORPHLINE_ID, ConfigValueType.STRING)
        .mandatoryPath(SCHEMA_CONFIG, ConfigValueType.OBJECT)
        .handlesOwnValidationPath(SCHEMA_CONFIG) 
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    return SchemaUtils.getSchemaComponents(config, configure, SCHEMA_CONFIG);
  }

}
