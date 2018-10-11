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
import com.cloudera.labs.envelope.utils.ConfigUtils;
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
import java.util.UUID;

/**
 * A planner implementation for appending the stream to the storage table. Only plans insert mutations.
 */
public class AppendPlanner implements BulkPlanner, ProvidesAlias, ProvidesValidations {

  public final static String KEY_FIELD_NAMES_CONFIG_NAME = "fields.key";
  public final static String LAST_UPDATED_FIELD_NAME_CONFIG_NAME = "field.last.updated";
  public final static String UUID_KEY_CONFIG_NAME = "uuid.key.enabled";

  private Config config;

  @Override
  public void configure(Config config) {
    this.config = config;
  }

  @Override
  public List<Tuple2<MutationType, Dataset<Row>>> planMutationsForSet(Dataset<Row> arriving)
  {
    if (setsKeyToUUID()) {
      arriving = arriving.withColumn(getKeyFieldNames().get(0), functions.lit(UUID.randomUUID().toString()));
    }

    if (hasLastUpdatedField()) {
      arriving = arriving.withColumn(getLastUpdatedFieldName(), functions.lit(currentTimestampString()));
    }

    List<Tuple2<MutationType, Dataset<Row>>> planned = Lists.newArrayList();

    planned.add(new Tuple2<MutationType, Dataset<Row>>(MutationType.INSERT, arriving));

    return planned;
  }

  @Override
  public Set<MutationType> getEmittedMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT);
  }

  private String currentTimestampString() {
    return new Date(System.currentTimeMillis()).toString();
  }

  private List<String> getKeyFieldNames() {
    return config.getStringList(KEY_FIELD_NAMES_CONFIG_NAME);
  }

  private boolean hasLastUpdatedField() {
    return config.hasPath(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
  }

  private String getLastUpdatedFieldName() {
    return config.getString(LAST_UPDATED_FIELD_NAME_CONFIG_NAME);
  }

  private boolean setsKeyToUUID() {
    return ConfigUtils.getOrElse(config, UUID_KEY_CONFIG_NAME, false);
  }

  @Override
  public String getAlias() {
    return "append";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(LAST_UPDATED_FIELD_NAME_CONFIG_NAME, ConfigValueType.STRING)
        .optionalPath(UUID_KEY_CONFIG_NAME, ConfigValueType.BOOLEAN)
        .ifPathExists(UUID_KEY_CONFIG_NAME,
            Validations.single().mandatoryPath(KEY_FIELD_NAMES_CONFIG_NAME, ConfigValueType.LIST))
        .build();
  }
  
}
