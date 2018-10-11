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

package com.cloudera.labs.envelope.plan;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import scala.Tuple2;

import java.util.Date;
import java.util.List;
import java.util.Set;

public class SystemTimeUpsertPlanner implements BulkPlanner, ProvidesAlias, ProvidesValidations {

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

  @Override
  public String getAlias() {
    return "upsert";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(LAST_UPDATED_FIELD_NAME_CONFIG_NAME, ConfigValueType.STRING)
        .build();
  }
  
}
