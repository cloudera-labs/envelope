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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Return a new <code>Dataset<Row></code> by filtering on an <code>IN</code> clause.
 * <p>
 * The values of the <code>IN</code> list may be either a literal or extracted from another dependency step.
 * <p>
 * If this has more than one dependency, the <code>step</code> parameter is required to identify the filtered
 * {@link Dataset}.
 * In addition, if the filtered <code>step</code> has more than one {@link Column}, this will use <code>field</code>
 * parameter to designate the filtered column, otherwise this will default to the first column in the
 * <code>Dataset</code>.
 * <p>
 * This expects a list for the inline values parameter, <code>values.literal</code>.
 * <p>
 * If this references a step for <code>IN</code> values, the <code>values.reference.step</code> parameter is required.
 * In addition, if the reference <code>step</code> has more than one <code>Column</code>, this will use
 * <code>values.reference.field</code> parameter to designate the filtering column, otherwise this will default to the
 * first column in the reference <code>Dataset</code>.
 * <p>
 * This will execute the <code>IN</code> list filtering by batching the referring values according to the value of the
 * <code>values.reference.batch-size</code> parameter.
 * This will default to 1000 if no value is specified.
 * <p>
 * Note: the <code>IN</code> list filtering is performed using the {@link Dataset#toLocalIterator()} method to marshal
 * to the list's values.
 * This operation is necessary to construct the <code>IN</code> filter (see <a href="https://issues.apache.org/jira/browse/SPARK-23945">SPARK-23954</a> for details).
 * If possible, a typical join operation is much more efficient and recommended.
 */

public class InListDeriver implements Deriver, ProvidesAlias, ProvidesValidations {

  public static final int DEFAULT_BATCH_SIZE = 1000;

  public static final String INLIST_DERIVER_ALIAS = "in-list";
  public static final String INLIST_STEP_CONFIG = "step";
  public static final String INLIST_FIELD_CONFIG = "field";
  public static final String INLIST_VALUES_CONFIG = "values.literal";
  public static final String INLIST_REFSTEP_CONFIG = "values.reference.step";
  public static final String INLIST_REFFIELD_CONFIG = "values.reference.field";
  public static final String INLIST_BATCH_SIZE = "values.reference.batch-size";

  private String stepName;
  private String fieldName;
  private String refStepName;
  private String refFieldName;
  private List<Object> inList;
  private long batchSize;

  private static final Logger LOGGER = LoggerFactory.getLogger(InListDeriver.class);

  @Override
  public void configure(Config config) {
    // Step name to query with the IN list
    this.stepName = ConfigUtils.getOrNull(config, INLIST_STEP_CONFIG);

    // Column to query with the IN list
    this.fieldName = ConfigUtils.getOrNull(config, INLIST_FIELD_CONFIG);

    // If the IN list is specified inline
    this.inList = ConfigUtils.getOrNull(config, INLIST_VALUES_CONFIG);

    // If the IN list is a referenced step dependency
    this.refStepName = ConfigUtils.getOrNull(config, INLIST_REFSTEP_CONFIG);

    // If the reference dependency has multiple columns
    this.refFieldName = ConfigUtils.getOrNull(config, INLIST_REFFIELD_CONFIG);

    // Get the batch size
    this.batchSize = ConfigUtils.getOrElse(config, INLIST_BATCH_SIZE, DEFAULT_BATCH_SIZE);
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) {

    Dataset<Row> target = getStepDataFrame(dependencies);
    if (target.columns().length < 1) {
      throw new RuntimeException("Targeted step, '" + stepName + ",' has no columns");
    }

    try {
      String targetField = fieldName == null ? target.columns()[0] : fieldName;
      Column targetColumn = target.col(targetField);

      LOGGER.debug("Targeting '{}[{}]'", stepName, targetField);

      // If the IN list is inline, there is no batch
      if (inList != null) {
        LOGGER.debug("IN list is inline");
        return target.filter(targetColumn.isin(inList.toArray()));
      }

      // Otherwise, collect the values from the reference, executed within the batch
      else {
        LOGGER.trace("IN list is a reference");
        Dataset<Row> reference = dependencies.get(refStepName);
        String referenceField = refFieldName == null ? reference.columns()[0] : refFieldName;

        LOGGER.debug("Referencing using {}[{}]", refStepName, referenceField);
        Column referenceColumn = reference.col(referenceField);

        Iterator<Row> referenceIterator = reference.select(referenceColumn).distinct().toLocalIterator();
        this.inList = new ArrayList<>();
        long counter = 0;

        // Set up the batch collector
        JavaRDD<Row> unionRDD = new JavaSparkContext(Contexts.getSparkSession().sparkContext()).emptyRDD();
        Dataset<Row> union = Contexts.getSparkSession().createDataFrame(unionRDD, target.schema());

        while (referenceIterator.hasNext()) {
          // Flush the batch
          if (counter == batchSize) {
            LOGGER.trace("Flushing batch");
            union = union.union(target.filter(targetColumn.isin(inList.toArray())));
            inList.clear();
            counter = 0L;
          }

          // Gather the elements of the IN list from the reference
          inList.add(referenceIterator.next().get(0));
          counter++;
        }

        // If the selection is under the batch threshold
        if (union.rdd().isEmpty()) {
          return target.filter(targetColumn.isin(inList.toArray()));
        }

        // Flush any remaining IN list values
        else {
          return union.union(target.filter(targetColumn.isin(inList.toArray())));
        }
      }
    } catch (Throwable ae) {
      throw new RuntimeException("Error executing IN list filtering", ae);
    }

  }

  private Dataset<Row> getStepDataFrame(Map<String, Dataset<Row>> dependencies) {
    if (stepName != null) {
      if (!dependencies.containsKey(stepName)) {
        throw new RuntimeException("In-list deriver does not have step '" + stepName + "' in its dependencies");
      }

      Dataset<Row> step = dependencies.get(stepName);

      if (step == null) {
        throw new RuntimeException("Targeted step, '" + stepName + "', is null");
      }

      return step;
    }
    else {
      if (dependencies.size() != 1) {
        throw new RuntimeException("In-list deriver must specify a step if it does not only have one dependency");
      }
      return dependencies.values().iterator().next();
    }
  }

  @Override
  public String getAlias() {
    return INLIST_DERIVER_ALIAS;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(INLIST_STEP_CONFIG, ConfigValueType.STRING)
        .optionalPath(INLIST_FIELD_CONFIG, ConfigValueType.STRING)
        .optionalPath(INLIST_REFSTEP_CONFIG, ConfigValueType.STRING)
        .optionalPath(INLIST_VALUES_CONFIG, ConfigValueType.LIST)
        .exactlyOnePathExists(INLIST_REFSTEP_CONFIG, INLIST_VALUES_CONFIG)
        .ifPathExists(INLIST_REFSTEP_CONFIG,
            Validations.single().optionalPath(INLIST_REFFIELD_CONFIG, ConfigValueType.STRING))
        .optionalPath(INLIST_BATCH_SIZE, ConfigValueType.NUMBER)
        .build();
  }

}
