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
package com.cloudera.labs.envelope.examples;

import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class FIXGenerator {

  private static Logger LOG = LoggerFactory.getLogger(FIXGenerator.class);

  public static void main(final String[] args) throws Exception {
    final Properties props = new Properties();
    props.put("bootstrap.servers", args[0]);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    int threads = 1;
    if (args.length == 3) {
      threads = Integer.parseInt(args[2]);
    }

    ExecutorService threadPool = Executors.newFixedThreadPool(threads);

    for (int t = 0; t < threads; t++) {
      threadPool.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          @SuppressWarnings("resource")
          KafkaProducer<String, String> producer = new KafkaProducer<>(props);

          Set<Order> openOrders = Sets.newHashSet();
          Set<Order> completedOrders = Sets.newHashSet();
          long lastReportedSecond = System.currentTimeMillis() / 1000;
          long recordsProducedSinceLastReport = 0;
          int secondsPerReport = 10;

          while(true) {
            while (openOrders.size() < 5000) {
              Order newOrder = new Order();
              String newOrderSingleFIX = newOrder.newOrderSingleFIX();
              producer.send(new ProducerRecord<String, String>(args[1], newOrderSingleFIX));
              recordsProducedSinceLastReport++;
              openOrders.add(newOrder);
            }

            for (Order order : openOrders) {
              if (!order.isComplete()) {
                if (order.hasFurtherExecuted()) {
                  String executionReportFIX = order.nextExecutionReportFIX();
                  producer.send(new ProducerRecord<String, String>(args[1], executionReportFIX));
                  recordsProducedSinceLastReport++;
                }
              }
              else {
                completedOrders.add(order);
              }
            }

            for (Order completedOrder : completedOrders) {
              openOrders.remove(completedOrder);
            }
            completedOrders.clear();

            long currentSecond = System.currentTimeMillis() / 1000;
            if (currentSecond >= lastReportedSecond + 10) {
              long rate = recordsProducedSinceLastReport / secondsPerReport;
              LOG.info("Generation rate: " + rate + " records/sec");
              lastReportedSecond = currentSecond;
              recordsProducedSinceLastReport = 0;
            }
          }
        }
      });
    }
  }

  private static class Order {
    private String clordid;
    private String orderid;
    private int orderqty;
    private int leavesqty;
    private Symbol symbol;

    private final String pairDelimiter = "\001";
    private final String kvDelimiter = "=";

    private enum Symbol {
      AAPL, MSFT, ORCL, VMW, GOOG, AMZN, FB, TWTR
    }

    public Order() {
      clordid = UUID.randomUUID().toString();
      orderid = UUID.randomUUID().toString();
      orderqty = new Random().nextInt(10000);
      leavesqty = orderqty;
      symbol = Symbol.values()[new Random().nextInt(Symbol.values().length)];
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
      message.append(constructKVP("60", System.currentTimeMillis()));
      message.append(constructKVP("38", orderqty));
      message.append(constructKVP("40", 2));
      message.append(constructKVP("10", "000"));

      return message.toString();
    }

    public boolean isComplete() { return leavesqty == 0; }

    public boolean hasFurtherExecuted() { return new Random().nextInt(100) == 0; }

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

      int execRptQty = new Random().nextInt(3000);
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
      message.append(constructKVP("6", new Random().nextFloat()));
      message.append(constructKVP("60", System.currentTimeMillis()));
      message.append(constructKVP("10", "000"));

      return message.toString();
    }

    private String constructKVP(String tag, Object value) {
      return tag + kvDelimiter + value + pairDelimiter;
    }
  }

}
