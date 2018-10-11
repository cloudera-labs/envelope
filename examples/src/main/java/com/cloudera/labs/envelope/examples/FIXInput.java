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

package com.cloudera.labs.envelope.examples;

import com.cloudera.labs.envelope.input.BatchInput;
import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class FIXInput implements BatchInput {

  private int tasks;
  private long ordersPerTask;

  @Override
  public void configure(Config config) {
    tasks = config.getInt("tasks");
    ordersPerTask = config.getLong("orders.per.task");
  }

  @Override
  public Dataset<Row> read() throws Exception {    
    JavaRDD<Long> baseRDD = Contexts.getSparkSession().range(tasks).javaRDD().repartition(tasks);
    
    JavaRDD<Row> fixRDD = baseRDD.flatMap(new GenerateFIXMessages(ordersPerTask));
    
    StructType schema = DataTypes.createStructType(Lists.newArrayList(DataTypes.createStructField("fix", DataTypes.StringType, false)));
    
    Dataset<Row> fixDF = Contexts.getSparkSession().createDataFrame(fixRDD, schema);
    
    return fixDF;
  }

  @SuppressWarnings("serial")
  private static class GenerateFIXMessages implements FlatMapFunction<Long, Row> {
    private long ordersPerTask;
    
    public GenerateFIXMessages(long ordersPerTask) {
      this.ordersPerTask = ordersPerTask;
    }
    
    @Override
    public Iterator<Row> call(Long ignored) throws Exception {
      List<Row> messages = Lists.newArrayList();
      
      for (int i = 0; i < ordersPerTask; i++) {
        Order newOrder = new Order();
        messages.add(RowFactory.create(newOrder.newOrderSingleFIX()));
        
        while (!newOrder.isComplete()) {
          messages.add(RowFactory.create(newOrder.nextExecutionReportFIX()));
        }
      }
      
      return messages.iterator();
    }
    
    private static class Order {
      private String clordid;
      private String orderid;
      private int orderqty;
      private int leavesqty;
      private Symbol symbol;
      private long transacttime;

      private final String pairDelimiter = "\001";
      private final String kvDelimiter = "=";
      
      private Random random = new Random();

      private enum Symbol {
        AAPL, MSFT, ORCL, VMW, GOOG, AMZN, FB, TWTR
      }

      public Order() {
        clordid = UUID.randomUUID().toString();
        orderid = UUID.randomUUID().toString();
        orderqty = random.nextInt(10000);
        leavesqty = orderqty;
        symbol = Symbol.values()[random.nextInt(Symbol.values().length)];
        transacttime = System.currentTimeMillis();
      }

      public String newOrderSingleFIX() {
        /*
          35: msgtype
          11: clordid
          21: handlinst
          55: symbol
          54: side
          60: transacttime
          38: orderqty
          40: ordtype
          10: checksum
        */

        StringBuilder message = new StringBuilder();

        message.append(constructKVP("35", "D"));
        message.append(constructKVP("11", clordid));
        message.append(constructKVP("21", 2));
        message.append(constructKVP("55", symbol));
        message.append(constructKVP("54", 2));
        message.append(constructKVP("60", transacttime));
        message.append(constructKVP("38", orderqty));
        message.append(constructKVP("40", 2));
        message.append(constructKVP("10", "000"));

        advanceThroughTime();

        return message.toString();
      }

      public boolean isComplete() { return leavesqty == 0; }

      public String nextExecutionReportFIX() {
        /*
          35: msgtype
          37: orderid
          11: clordid
          17: execid
          20: exectranstype
          150: exectype
          39: ordstatus
          55: symbol
          54: side
          151: leavesqty
          14: cumqty
          6: avgpx
          60: transacttime
          10: checksum
         */

        int execRptQty = random.nextInt(3000);
        leavesqty -= execRptQty;
        if (leavesqty < 0) leavesqty = 0;

        StringBuilder message = new StringBuilder();

        message.append(constructKVP("35", "8"));
        message.append(constructKVP("37", orderid));
        message.append(constructKVP("11", clordid));
        message.append(constructKVP("17", UUID.randomUUID()));
        message.append(constructKVP("20", 0));
        message.append(constructKVP("150", 0));
        message.append(constructKVP("39", leavesqty == 0 ? 2 : 1));
        message.append(constructKVP("55", symbol));
        message.append(constructKVP("54", 1));
        message.append(constructKVP("151", leavesqty));
        message.append(constructKVP("14", orderqty - leavesqty));
        message.append(constructKVP("6", random.nextFloat()));
        message.append(constructKVP("60", transacttime));
        message.append(constructKVP("10", "000"));

        advanceThroughTime();

        return message.toString();
      }

      private void advanceThroughTime() {
        transacttime += (random.nextInt(10) + 1);
      }

      private String constructKVP(String tag, Object value) {
        return tag + kvDelimiter + value + pairDelimiter;
      }
    }
  }

}
