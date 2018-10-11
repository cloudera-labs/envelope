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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.derive.dq.DatasetRule;
import com.cloudera.labs.envelope.derive.dq.DatasetRuleFactory;
import com.cloudera.labs.envelope.derive.dq.RowRule;
import com.cloudera.labs.envelope.derive.dq.RowRuleFactory;
import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Deriver which allows the Datasets to be checked against a list of configured data quality rules.
 *
 * The deriver can operate at a whole dataset level (e.g. count) or on a row-level (e.g. fields match
 * a regex).
 */
public class DataQualityDeriver
    implements Deriver, ProvidesAlias, ProvidesValidations, InstantiatesComponents {

  public static final String SCOPE_CONFIG = "scope";
  public static final String RULES_CONFIG = "rules";
  public static final String DATASET_CONFIG = "dataset";
  public static final String RESULTS_FIELD_CONFIG = "resultsfield";

  private static final String DEFAULT_RESULTS_FIELD = "results";

  @Override
  public String getAlias() {
    return "dq";
  }

  private enum Scope {
    DATASET,
    ROW
  }

  private Scope scope;
  private Map<String, DatasetRule> datasetRules = new HashMap<>();
  private Map<String, RowRule> rowRules = new HashMap<>();
  private String dataset = "";
  private String resultsField = DEFAULT_RESULTS_FIELD;

  @Override
  public void configure(Config config) {
    scope = Scope.valueOf(config.getString(SCOPE_CONFIG).toUpperCase());

    if (config.hasPath(DATASET_CONFIG)) {
      dataset = config.getString(DATASET_CONFIG);
    }

    Config rulesConfig = config.getConfig(RULES_CONFIG);
    if (scope == Scope.DATASET) {
      datasetRules = getDatasetRules(rulesConfig, true);
    } else {
      if (config.hasPath(RESULTS_FIELD_CONFIG)) {
        resultsField = config.getString(RESULTS_FIELD_CONFIG);
      }
      rowRules = getRowRules(rulesConfig, true);
    }
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    if (dependencies.size() > 1 && dataset.isEmpty()) {
      throw new RuntimeException("Must specify dataset on which to conduct data quality tests when more than one dependency");
    }
    Dataset<Row> theDataset;
    Dataset<Row> theResults = null;
    if (dependencies.size() == 1) {
      theDataset = dependencies.values().iterator().next();
    } else {
      theDataset = dependencies.get(dataset);
    }
    if (scope == Scope.DATASET) {
      // The checks are run at a dataset level and we are simply returning a DS of <name, boolean> Rows
      for (DatasetRule rule : datasetRules.values()) {
        if (theResults == null) {
          theResults = rule.check(theDataset, dependencies);
        } else {
          theResults = theResults.union(rule.check(theDataset, dependencies));
        }
      }
    } else {
      if (theDataset.schema().getFieldIndex(resultsField).isDefined()) {
        throw new RuntimeException("The field [" + resultsField + "] already exists in the dataset schema. Use the " +
            RESULTS_FIELD_CONFIG + " configuration parameter to customize the data quality check field name");
      }
      List<StructField> checkField = Lists.newArrayList(
          new StructField(resultsField,
              DataTypes.createMapType(DataTypes.StringType, DataTypes.BooleanType),
              false, Metadata.empty()));
      theResults = theDataset.map(new CheckRowRules(rowRules, resultsField),
          RowEncoder.apply(RowUtils.appendFields(theDataset.schema(), checkField)));
    }

    return theResults;
  }

  private Map<String, DatasetRule> getDatasetRules(Config rulesConfig, boolean configure) {
    Map<String, DatasetRule> datasetRules = Maps.newHashMap();

    for (String rule : rulesConfig.root().keySet()) {
      datasetRules.put(rule, DatasetRuleFactory.create(rule, rulesConfig.getConfig(rule), configure));
    }

    return datasetRules;
  }

  private Map<String, RowRule> getRowRules(Config rulesConfig, boolean configure) {
    Map<String, RowRule> rowRules = Maps.newHashMap();

    for (String rule : rulesConfig.root().keySet()) {
      rowRules.put(rule, RowRuleFactory.create(rule, rulesConfig.getConfig(rule), configure));
    }

    return rowRules;
  }

  @SuppressWarnings("serial")
  private static class CheckRowRules implements MapFunction<Row, Row> {
    private Map<String, RowRule> rules;
    private String resultsFieldName;

    CheckRowRules(Map<String, RowRule> rules, String resultsFieldName) {
      this.rules = rules;
      this.resultsFieldName = resultsFieldName;
    }

    @Override
    public Row call(Row row) throws Exception {
      Map<String, Boolean> ruleOutcomes = new HashMap<>();
      for (Map.Entry<String, RowRule> rule : rules.entrySet()) {
        ruleOutcomes.put(rule.getKey(), rule.getValue().check(row));
      }

      return RowUtils.append(row, resultsFieldName,
          DataTypes.createMapType(DataTypes.StringType, DataTypes.BooleanType), toScalaMap(ruleOutcomes));
    }

    private static <A,B> scala.collection.mutable.Map<A,B> toScalaMap(java.util.Map<A,B> jMap) {
      return JavaConverters.mapAsScalaMapConverter(jMap).asScala();
    }
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(SCOPE_CONFIG, ConfigValueType.STRING)
        .mandatoryPath(RULES_CONFIG, ConfigValueType.OBJECT)
        .optionalPath(DATASET_CONFIG, ConfigValueType.STRING)
        .optionalPath(RESULTS_FIELD_CONFIG, ConfigValueType.STRING)
        .handlesOwnValidationPath(RULES_CONFIG)
        .build();
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    Set<InstantiatedComponent> components = Sets.newHashSet();

    Scope scope = Scope.valueOf(config.getString(SCOPE_CONFIG).toUpperCase());

    if (scope == Scope.DATASET) {
      Map<String, DatasetRule> datasetRules = getDatasetRules(config.getConfig(RULES_CONFIG), configure);
      for (Entry<String, DatasetRule> datasetRuleEntry : datasetRules.entrySet()) {
        String ruleName = datasetRuleEntry.getKey();
        DatasetRule rule = datasetRuleEntry.getValue();

        components.add(new InstantiatedComponent(
            rule, config.getConfig(RULES_CONFIG).getConfig(ruleName), "Rule " + ruleName));
      }
    }
    else {
      Map<String, RowRule> rowRules = getRowRules(config.getConfig(RULES_CONFIG), configure);
      for (Entry<String, RowRule> rowRuleEntry : rowRules.entrySet()) {
        String ruleName = rowRuleEntry.getKey();
        RowRule rule = rowRuleEntry.getValue();

        components.add(new InstantiatedComponent(
            rule, config.getConfig(RULES_CONFIG).getConfig(ruleName), "Rule " + ruleName));
      }
    }

    return components;
  }

}
