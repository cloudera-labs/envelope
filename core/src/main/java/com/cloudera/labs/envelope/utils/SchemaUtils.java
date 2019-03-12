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

package com.cloudera.labs.envelope.utils;

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.schema.Schema;
import com.cloudera.labs.envelope.schema.SchemaFactory;
import com.cloudera.labs.envelope.translate.Translator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

import java.util.List;
import java.util.Set;

public class SchemaUtils {

  public static StructType stringValueSchema() {
    return DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(Translator.VALUE_FIELD_NAME, DataTypes.StringType, false)
    });
  }

  public static StructType binaryValueSchema() {
    return DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(Translator.VALUE_FIELD_NAME, DataTypes.BinaryType, false)
    });
  }

  public static StructType subsetSchema(StructType schema, final List<String> fieldNames) {
    Seq<StructField> fieldSeq = schema.toTraversable().filter(new AbstractFunction1<StructField, Object>() {
      @Override
      public Object apply(StructField field) {
        return fieldNames.contains(field.name());
      }
    }).toSeq();

    StructType subset = DataTypes.createStructType(JavaConversions.seqAsJavaList(fieldSeq));

    return subset;
  }

  public static StructType subtractSchema(StructType schema, List<String> subtractFieldNames) {
    List<String> fieldNames = Lists.newArrayList();

    for (StructField schemaField : schema.fields()) {
      if (!subtractFieldNames.contains(schemaField.name())) {
        fieldNames.add(schemaField.name());
      }
    }

    StructType subtracted = subsetSchema(schema, fieldNames);

    return subtracted;
  }

  public static StructType appendFields(StructType from, List<StructField> fields) {
    StructType to = DataTypes.createStructType(from.fields());

    for (StructField field : fields) {
      to = to.add(field);
    }

    return to;
  }

  public static Set<InstantiatedComponent> getSchemaComponents(Config config,
      boolean configure, String ...schemaConfigPaths) {

    Set<InstantiatedComponent> components = Sets.newHashSet();

    for (String schemaConfigPath : schemaConfigPaths) {
      Config schemaConfig = config.getConfig(schemaConfigPath);
      Schema schema = SchemaFactory.create(schemaConfig, configure);
      components.add(new InstantiatedComponent(
        schema, config.getConfig(schemaConfigPath), schemaConfigPath));
    }

    return components;
  }

}
