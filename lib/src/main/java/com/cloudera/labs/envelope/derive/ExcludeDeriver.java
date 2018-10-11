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

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConversions;

import java.util.List;
import java.util.Map;

/**
 * <p>Execute a LEFT ANTI JOIN on the designated dataset, returning only the rows of the designated compareDataset that do not
 * match on the given fields of the other dataset.</p>
 * <p>Equivalent to running the following Spark SQL query:</p>
 * <pre>
 *   SELECT ExclusionCompare.*
 *   FROM ExclusionCompare LEFT ANTI JOIN ExclusionWith
 *   USING (fieldA, fieldB)
 * </pre>
 * <p>Note that both datasets must have identically named columns/fields in the USING statement.</p>
 */
public class ExcludeDeriver implements Deriver, ProvidesAlias, ProvidesValidations {

  public static final String EXCLUSION_COMPARE_CONFIG = "compare";
  public static final String EXCLUSION_WITH_CONFIG = "with";
  public static final String EXCLUSION_FIELDS_CONFIG = "field.names";

  private String compareDataset;
  private String withDataset;
  private List<String> fields;

  @Override
  public void configure(Config config) {
    this.compareDataset = config.getString(EXCLUSION_COMPARE_CONFIG);
    this.withDataset = config.getString(EXCLUSION_WITH_CONFIG);
    this.fields = config.getStringList(EXCLUSION_FIELDS_CONFIG);
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    Dataset<Row> compare, with;

    if (!dependencies.containsKey(compareDataset)) {
      throw new RuntimeException("Designated comparison target dataset is not a dependency: " + compareDataset);
    } else {
      compare = dependencies.get(compareDataset);
    }

    if (!dependencies.containsKey(withDataset)) {
      throw new RuntimeException("Designated comparison reference dataset is not a dependency: " + withDataset);
    } else {
      with = dependencies.get(withDataset);
    }

    return compare.join(with, JavaConversions.asScalaBuffer(fields).toList(), "leftanti");
  }

  @Override
  public String getAlias() {
    return "exclude";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(EXCLUSION_COMPARE_CONFIG, ConfigValueType.STRING)
        .mandatoryPath(EXCLUSION_FIELDS_CONFIG, ConfigValueType.LIST)
        .mandatoryPath(EXCLUSION_WITH_CONFIG, ConfigValueType.STRING)
        .build();
  }
  
}
