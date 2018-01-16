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
package com.cloudera.labs.envelope.derive;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

public class PivotDeriver implements Deriver {
  
  public static final String STEP_NAME_CONFIG = "step.name";
  public static final String ENTITY_KEY_FIELD_NAMES_CONFIG = "entity.key.field.names";
  public static final String PIVOT_KEY_FIELD_NAME_CONFIG = "pivot.key.field.name";
  public static final String PIVOT_VALUE_FIELD_NAME_CONFIG = "pivot.value.field.name";
  public static final String PIVOT_KEYS_SOURCE_CONFIG = "pivot.keys.source";
  public static final String PIVOT_KEYS_SOURCE_STATIC = "static";
  public static final String PIVOT_KEYS_SOURCE_DYNAMIC = "dynamic";
  public static final String PIVOT_KEYS_LIST_CONFIG = "pivot.keys.list";
  
  private String stepName;
  private List<String> entityKeyFieldNames;
  private String pivotKeyFieldName;
  private String pivotValueFieldName;
  private String pivotKeysSource;
  private List<String> pivotKeys;

  @Override
  public void configure(Config config) {
    ConfigUtils.assertConfig(config, STEP_NAME_CONFIG);
    ConfigUtils.assertConfig(config, ENTITY_KEY_FIELD_NAMES_CONFIG);
    ConfigUtils.assertConfig(config, PIVOT_KEY_FIELD_NAME_CONFIG);
    ConfigUtils.assertConfig(config, PIVOT_VALUE_FIELD_NAME_CONFIG);
    
    stepName = config.getString(STEP_NAME_CONFIG);
    entityKeyFieldNames = config.getStringList(ENTITY_KEY_FIELD_NAMES_CONFIG);
    pivotKeyFieldName = config.getString(PIVOT_KEY_FIELD_NAME_CONFIG);
    pivotValueFieldName = config.getString(PIVOT_VALUE_FIELD_NAME_CONFIG);
    
    if (config.hasPath(PIVOT_KEYS_SOURCE_CONFIG)) {
      pivotKeysSource = config.getString(PIVOT_KEYS_SOURCE_CONFIG);
      
      if (!pivotKeysSource.equals(PIVOT_KEYS_SOURCE_STATIC) && !pivotKeysSource.equals(PIVOT_KEYS_SOURCE_DYNAMIC)) {
        throw new RuntimeException("Pivot deriver values source must be '" + PIVOT_KEYS_SOURCE_STATIC +
            "' or '" + PIVOT_KEYS_SOURCE_DYNAMIC + "'");
      }
      
      if (pivotKeysSource.equals(PIVOT_KEYS_SOURCE_STATIC)) {
        ConfigUtils.assertConfig(config, PIVOT_KEYS_LIST_CONFIG);
        pivotKeys = config.getStringList(PIVOT_KEYS_LIST_CONFIG);
      }
    }
    else {
      pivotKeysSource = PIVOT_KEYS_SOURCE_DYNAMIC;
    }
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    if (!dependencies.containsKey(stepName)) {
      throw new RuntimeException("Pivot deriver references step that this step is not dependent on");
    }
    
    Dataset<Row> sourceStep = dependencies.get(stepName);

    RelationalGroupedDataset grouped;
    if (entityKeyFieldNames.size() == 1) {
      grouped = sourceStep.groupBy(entityKeyFieldNames.get(0));
    }
    else {
      grouped = sourceStep.groupBy(entityKeyFieldNames.get(0),
          Arrays.copyOfRange(entityKeyFieldNames.toArray(), 1, entityKeyFieldNames.size(), String[].class));
    }
    
    RelationalGroupedDataset pivotGrouped;
    if (pivotKeysSource.equals(PIVOT_KEYS_SOURCE_DYNAMIC)) {
      pivotGrouped = grouped.pivot(pivotKeyFieldName);
    }
    else {
      pivotGrouped = grouped.pivot(pivotKeyFieldName, Lists.<Object>newArrayList(pivotKeys));
    }

    Map<String, String> firstExpr = Maps.newHashMap();
    firstExpr.put(pivotValueFieldName, "first");

    Dataset<Row> pivoted = pivotGrouped.agg(firstExpr);


    return pivoted;
  }

}
