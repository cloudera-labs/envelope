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

import com.cloudera.labs.envelope.translate.Translator;
import com.google.common.collect.Lists;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

import java.util.List;

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

  public static StructType structTypeFor(List<String> fieldNames, List<String> fieldTypes) {
    List<StructField> fields = Lists.newArrayList();

    for (int i = 0; i < fieldNames.size(); i++) {
      String fieldName = fieldNames.get(i);
      String fieldType = fieldTypes.get(i);

      StructField field;
      switch (fieldType) {
        case "string":
          field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
          break;
        case "float":
          field = DataTypes.createStructField(fieldName, DataTypes.FloatType, true);
          break;
        case "double":
          field = DataTypes.createStructField(fieldName, DataTypes.DoubleType, true);
          break;
        case "byte":
          field = DataTypes.createStructField(fieldName, DataTypes.ByteType, true);
          break;
        case "short":
          field = DataTypes.createStructField(fieldName, DataTypes.ShortType, true);
          break;
        case "int":
          field = DataTypes.createStructField(fieldName, DataTypes.IntegerType, true);
          break;
        case "long":
          field = DataTypes.createStructField(fieldName, DataTypes.LongType, true);
          break;
        case "boolean":
          field = DataTypes.createStructField(fieldName, DataTypes.BooleanType, true);
          break;
        case "binary":
          field = DataTypes.createStructField(fieldName, DataTypes.BinaryType, true);
          break;
        case "timestamp":
          field = DataTypes.createStructField(fieldName, DataTypes.TimestampType, true);
          break;
        default:
          throw new RuntimeException("Unsupported provided field type: " + fieldType);
      }

      fields.add(field);
    }

    StructType schema = DataTypes.createStructType(fields);

    return schema;
  }

}
