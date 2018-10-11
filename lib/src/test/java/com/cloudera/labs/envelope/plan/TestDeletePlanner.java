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

import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestDeletePlanner {

  @Test
  public void testPlanner() {
    List<Row> rows = Lists.newArrayList(RowFactory.create("a", 1, false), RowFactory.create("b", 2, true));
    StructType schema = new StructType(new StructField[] {
        new StructField("field1", DataTypes.StringType, false, null),
        new StructField("field2", DataTypes.IntegerType, false, null),
        new StructField("field3", DataTypes.BooleanType, false, null)
    });
    
    Dataset<Row> data = Contexts.getSparkSession().createDataFrame(rows, schema);
    
    BulkPlanner p = new DeletePlanner();
    p.configure(ConfigFactory.empty());
    
    List<Tuple2<MutationType, Dataset<Row>>> planned = p.planMutationsForSet(data);
    
    assertEquals(1, planned.size());
    assertEquals(MutationType.DELETE, planned.get(0)._1());
    assertEquals(data, planned.get(0)._2());
  }

}
