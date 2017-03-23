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
package com.cloudera.labs.envelope.plan.bulk;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import com.cloudera.labs.envelope.plan.BulkPlanner;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.OverwritePlanner;

import scala.Tuple2;

public class TestOverwritePlanner {

  @Test
  public void testPlanner() {
    SparkConf conf = new SparkConf();
    conf.setMaster("local[1]");
    conf.setAppName("TestInsertOverwritePlanner.testPlanner");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlc = new SQLContext(jsc);

    DataFrame testData = sqlc.sql("SELECT 'test'");

    BulkPlanner planner = new OverwritePlanner();
    List<Tuple2<MutationType, DataFrame>> plan = planner.planMutationsForSet(testData);

    assertEquals(plan.size(), 1);
    assertEquals(plan.get(0)._1(), MutationType.OVERWRITE);
    assertEquals(plan.get(0)._2(), testData);

    jsc.close();
  }

}
