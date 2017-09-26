/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.plan;

import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

public class SystemTimeUpsertPlanner implements BulkPlanner {

  public final static String LAST_UPDATED_FIELD_NAME_CONFIG_NAME = "field.last.updated";

  private Config config;

  @Override
  public void configure(Config config) {
    this.config = config;
  }

  @Override
  public List<Tuple2<MutationType, Dataset<Row>>> planMutationsForSet(Dataset<Row> arriving)
  {
    if (hasLastUpdatedField()) {
      arriving = arriving.withColumn(getLastUpdatedFieldName(), functions.lit(currentTimestampString()));
    }

    List<Tuple2<MutationType, Dataset<Row>>> planned = Lists.newArrayList();

    planned.add(new Tuple2<MutationType, Dataset<Row>>(MutationType.UPSERT, arriving));

    return planned;
  }

  @Override
  public Set<MutationType> getEmittedMutationTypes() {
    return Sets.newHashSet(MutationType.UPSERT);
  }

  private String currentTimestampString() {
    return new Date(System.currentTimeMillis()).toString();
  }

  private boolean hasLastUpdatedField() {
    return config.hasPath(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
  }

  private String getLastUpdatedFieldName() {
    return config.getString(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
  }

}
