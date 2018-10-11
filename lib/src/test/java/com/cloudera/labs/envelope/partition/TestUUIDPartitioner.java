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

package com.cloudera.labs.envelope.partition;

import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUUIDPartitioner {

  @Test
  public void testNumPartitions() {
    ConfigurablePartitioner p = new UUIDPartitioner();
    
    JavaPairRDD<Row, Row> rdd = 
        Contexts.getSparkSession().range(10).javaRDD().map(new LongToRowFunction()).mapToPair(new UUIDKeyFunction()).repartition(10);
    
    p.configure(ConfigFactory.empty(), rdd);
    
    assertEquals(p.numPartitions(), 10);
  }
  
  @Test
  public void testLowerCaseUUID() {
    ConfigurablePartitioner p = new UUIDPartitioner();
    
    JavaPairRDD<Row, Row> base = Contexts.getSparkSession()
    .createDataFrame(
      Lists.<Row>newArrayList(
          RowFactory.create("00e3ddbf-22e3-430b-b1f1-8c90ff66070a"),
          RowFactory.create("aab6152d-bf3b-49ae-b2ae-830fa8c5a9f5"),
          RowFactory.create("176c4abb-482f-403a-acc1-bf2fce4b7e02"),
          RowFactory.create("ebc0a57b-4492-4f77-8585-e5ec031c77b8"),
          RowFactory.create("7fb15b26-e2e6-41aa-a3dd-911272bfca6c"),
          RowFactory.create("80942cf3-4f1c-4a6f-8ec3-df8e471d61c9"),
          RowFactory.create("cd70533a-f13c-4fc0-8609-86c1c892a078"),
          RowFactory.create("bc454e0f-e54b-44e4-9278-75a2dba25833"),
          RowFactory.create("62d71924-5c69-4771-8f78-0e518fae137d"),
          RowFactory.create("ff6c7c94-7fd7-4881-bc7f-ffd4713411a3")),
      DataTypes.createStructType(Lists.newArrayList(DataTypes.createStructField("uuid", DataTypes.StringType, false))))
    .javaRDD()
    .mapToPair(new CopyKeyFunction())
    .repartition(10);
    
    p.configure(ConfigFactory.empty(), base);
    
    int[] partitionIds = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    List<Tuple2<Row, Iterable<Row>>>[] results = base.groupByKey(p).collectPartitions(partitionIds);
    
    assertEquals(p.numPartitions(), 10);
    assertEquals(base.getNumPartitions(), 10);
    assertEquals(results.length, 10);
    
    assertEquals(results[0].size(), 2);
    assertEquals(results[1].size(), 0);
    assertEquals(results[2].size(), 0);
    assertEquals(results[3].size(), 1);
    assertEquals(results[4].size(), 1);
    assertEquals(results[5].size(), 1);
    assertEquals(results[6].size(), 1);
    assertEquals(results[7].size(), 1);
    assertEquals(results[8].size(), 1);
    assertEquals(results[9].size(), 2);
    
    // We can't assume the partitions are sorted
    assertTrue(results[0].get(0)._1().getString(0).equals("00e3ddbf-22e3-430b-b1f1-8c90ff66070a") ||
               results[0].get(1)._1().getString(0).equals("00e3ddbf-22e3-430b-b1f1-8c90ff66070a"));
    assertTrue(results[0].get(0)._1().getString(0).equals("176c4abb-482f-403a-acc1-bf2fce4b7e02") ||
               results[0].get(1)._1().getString(0).equals("176c4abb-482f-403a-acc1-bf2fce4b7e02"));
    assertEquals(results[3].get(0)._1().getString(0), "62d71924-5c69-4771-8f78-0e518fae137d");
    assertEquals(results[4].get(0)._1().getString(0), "7fb15b26-e2e6-41aa-a3dd-911272bfca6c");
    assertEquals(results[5].get(0)._1().getString(0), "80942cf3-4f1c-4a6f-8ec3-df8e471d61c9");
    assertEquals(results[6].get(0)._1().getString(0), "aab6152d-bf3b-49ae-b2ae-830fa8c5a9f5");
    assertEquals(results[7].get(0)._1().getString(0), "bc454e0f-e54b-44e4-9278-75a2dba25833");
    assertEquals(results[8].get(0)._1().getString(0), "cd70533a-f13c-4fc0-8609-86c1c892a078");
    assertTrue(results[9].get(0)._1().getString(0).equals("ebc0a57b-4492-4f77-8585-e5ec031c77b8") ||
               results[9].get(1)._1().getString(0).equals("ebc0a57b-4492-4f77-8585-e5ec031c77b8"));
    assertTrue(results[9].get(0)._1().getString(0).equals("ff6c7c94-7fd7-4881-bc7f-ffd4713411a3") ||
               results[9].get(1)._1().getString(0).equals("ff6c7c94-7fd7-4881-bc7f-ffd4713411a3"));
  }
  
  @Test
  public void testUpperCaseUUID() {
    ConfigurablePartitioner p = new UUIDPartitioner();
    
    JavaPairRDD<Row, Row> base = Contexts.getSparkSession()
    .createDataFrame(
      Lists.<Row>newArrayList(
          RowFactory.create("00E3DDBF-22E3-430B-B1F1-8C90FF66070A"),
          RowFactory.create("AAB6152D-BF3B-49AE-B2AE-830FA8C5A9F5"),
          RowFactory.create("176C4ABB-482F-403A-ACC1-BF2FCE4B7E02"),
          RowFactory.create("EBC0A57B-4492-4F77-8585-E5EC031C77B8"),
          RowFactory.create("7FB15B26-E2E6-41AA-A3DD-911272BFCA6C"),
          RowFactory.create("80942CF3-4F1C-4A6F-8EC3-DF8E471D61C9"),
          RowFactory.create("CD70533A-F13C-4FC0-8609-86C1C892A078"),
          RowFactory.create("BC454E0F-E54B-44E4-9278-75A2DBA25833"),
          RowFactory.create("62D71924-5C69-4771-8F78-0E518FAE137D"),
          RowFactory.create("FF6C7C94-7FD7-4881-BC7F-FFD4713411A3")),
      DataTypes.createStructType(Lists.newArrayList(DataTypes.createStructField("uuid", DataTypes.StringType, false))))
    .javaRDD()
    .mapToPair(new CopyKeyFunction())
    .repartition(10);
    
    p.configure(ConfigFactory.empty(), base);
    
    int[] partitionIds = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    List<Tuple2<Row, Iterable<Row>>>[] results = base.groupByKey(p).collectPartitions(partitionIds);
    
    assertEquals(p.numPartitions(), 10);
    assertEquals(base.getNumPartitions(), 10);
    assertEquals(results.length, 10);
    
    assertEquals(results[0].size(), 2);
    assertEquals(results[1].size(), 0);
    assertEquals(results[2].size(), 0);
    assertEquals(results[3].size(), 1);
    assertEquals(results[4].size(), 1);
    assertEquals(results[5].size(), 1);
    assertEquals(results[6].size(), 1);
    assertEquals(results[7].size(), 1);
    assertEquals(results[8].size(), 1);
    assertEquals(results[9].size(), 2);
    
    // We can't assume the partitions are sorted
    assertTrue(results[0].get(0)._1().getString(0).equals("00E3DDBF-22E3-430B-B1F1-8C90FF66070A") ||
               results[0].get(1)._1().getString(0).equals("00E3DDBF-22E3-430B-B1F1-8C90FF66070A"));
    assertTrue(results[0].get(0)._1().getString(0).equals("176C4ABB-482F-403A-ACC1-BF2FCE4B7E02") ||
               results[0].get(1)._1().getString(0).equals("176C4ABB-482F-403A-ACC1-BF2FCE4B7E02"));
    assertEquals(results[3].get(0)._1().getString(0), "62D71924-5C69-4771-8F78-0E518FAE137D");
    assertEquals(results[4].get(0)._1().getString(0), "7FB15B26-E2E6-41AA-A3DD-911272BFCA6C");
    assertEquals(results[5].get(0)._1().getString(0), "80942CF3-4F1C-4A6F-8EC3-DF8E471D61C9");
    assertEquals(results[6].get(0)._1().getString(0), "AAB6152D-BF3B-49AE-B2AE-830FA8C5A9F5");
    assertEquals(results[7].get(0)._1().getString(0), "BC454E0F-E54B-44E4-9278-75A2DBA25833");
    assertEquals(results[8].get(0)._1().getString(0), "CD70533A-F13C-4FC0-8609-86C1C892A078");
    assertTrue(results[9].get(0)._1().getString(0).equals("EBC0A57B-4492-4F77-8585-E5EC031C77B8") ||
               results[9].get(1)._1().getString(0).equals("EBC0A57B-4492-4F77-8585-E5EC031C77B8"));
    assertTrue(results[9].get(0)._1().getString(0).equals("FF6C7C94-7FD7-4881-BC7F-FFD4713411A3") ||
               results[9].get(1)._1().getString(0).equals("FF6C7C94-7FD7-4881-BC7F-FFD4713411A3"));
  }
  
  @SuppressWarnings("serial")
  private static class LongToRowFunction implements Function<Long, Row> {
    @Override
    public Row call(Long longValue) throws Exception {
      return RowFactory.create(longValue);
    }
  }
  
  @SuppressWarnings("serial")
  private static class CopyKeyFunction implements PairFunction<Row, Row, Row> {
    @Override
    public Tuple2<Row, Row> call(Row t) throws Exception {
      return new Tuple2<Row, Row>(t, t);
    }
  }
  
  @SuppressWarnings("serial")
  private static class UUIDKeyFunction implements PairFunction<Row, Row, Row> {
    @Override
    public Tuple2<Row, Row> call(Row t) throws Exception {
      String key = UUID.randomUUID().toString();
      return new Tuple2<Row, Row>(RowFactory.create(key), t);
    }
  }
  
}
