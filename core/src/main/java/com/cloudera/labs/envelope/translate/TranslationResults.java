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

package com.cloudera.labs.envelope.translate;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

public class TranslationResults {

  private Dataset<Row> results;
  private StructType errorSchema;
  private StructType successSchema;

  public TranslationResults(Dataset<Row> results, StructType successSchema, StructType errorSchema) {
    this.results = results;
    this.successSchema = successSchema;
    this.errorSchema = errorSchema;
  }

  public Dataset<Row> getTranslated() {
    return filterOnHadError(results, false).drop(new Column(TranslateFunction.HAD_ERROR_FIELD_NAME));
  }

  public Dataset<Row> getErrors() {
    return filterOnHadError(results, true).drop(new Column(TranslateFunction.HAD_ERROR_FIELD_NAME));
  }

  private Dataset<Row> filterOnHadError(Dataset<Row> results, boolean hadError) {
    return results.filter(new FilterOnHadErrorFunction(hadError)).
        map(new applyEncoding(),RowEncoder.apply(getEncoding(hadError)));
  }

  private StructType getEncoding(boolean hadError) {
    if (hadError) {
      return errorSchema;
    }
    else {
      return successSchema;
    }
  }

  @SuppressWarnings("serial")
  private static class FilterOnHadErrorFunction implements FilterFunction<Row> {
    private boolean hadError;

    FilterOnHadErrorFunction(boolean hadError) {
      this.hadError = hadError;
    }

    @Override
    public boolean call(Row row) {
      return row.getBoolean(row.fieldIndex(TranslateFunction.HAD_ERROR_FIELD_NAME)) == this.hadError;
    }
  }

  @SuppressWarnings("serial")
  private static class applyEncoding implements MapFunction<Row,Row> {
    @Override
    public Row call(Row row) {
      return row;
    }
  }

}
