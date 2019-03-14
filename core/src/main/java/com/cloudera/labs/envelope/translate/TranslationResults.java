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

import com.cloudera.labs.envelope.spark.Contexts;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class TranslationResults {

  private JavaRDD<Row> results;
  private StructType errorSchema;
  private StructType translatedSchema;

  public TranslationResults(JavaRDD<Row> results, StructType translatedSchema, StructType errorSchema) {
    this.results = results;
    this.translatedSchema = translatedSchema;
    this.errorSchema = errorSchema;
  }

  public Dataset<Row> getTranslated() {
    return Contexts.getSparkSession()
        .createDataFrame(filterOnHadError(results, false), translatedSchema)
        .drop(TranslateFunction.HAD_ERROR_FIELD_NAME);
  }

  public Dataset<Row> getErrored() {
    return Contexts.getSparkSession()
        .createDataFrame(filterOnHadError(results, true), errorSchema)
        .drop(TranslateFunction.HAD_ERROR_FIELD_NAME);
  }

  private JavaRDD<Row> filterOnHadError(JavaRDD<Row> results, boolean hadError) {
    return results.filter(new FilterOnHadErrorFunction(hadError));
  }

  @SuppressWarnings("serial")
  private static class FilterOnHadErrorFunction implements Function<Row, Boolean> {
    private boolean hadError;

    FilterOnHadErrorFunction(boolean hadError) {
      this.hadError = hadError;
    }

    @Override
    public Boolean call(Row row) {
      return this.hadError == row.<Boolean>getAs(TranslateFunction.HAD_ERROR_FIELD_NAME);
    }
  }

}
