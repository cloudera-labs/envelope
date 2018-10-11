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

package com.cloudera.labs.envelope.derive.dq;

import com.cloudera.labs.envelope.component.Component;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Map;

public interface DatasetRule extends Component, Serializable {

  StructType SCHEMA = new StructType(new StructField[] {
      new StructField("name", DataTypes.StringType, false, Metadata.empty()),
      new StructField("result", DataTypes.BooleanType, false, Metadata.empty())
  });

  /**
   * Called once by Envelope to configure the data quality rule
   * @param name name of the rule
   * @param config configuration for the rule
   */
  void configure(String name, Config config);

  /**
   * Apply the rule to the supplied dataset and return
   * @param dataset the {@link Dataset} on which to run the check
   * @return pass or fail
   */
  Dataset<Row> check(Dataset<Row> dataset, Map<String, Dataset<Row>> stepDependencies);

}
