package com.cloudera.labs.envelope.examples;

import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.Sets;

public class FIXGenerator {
    
    public static void main(final String[] args) throws Exception {
        final Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        @SuppressWarnings("resource")
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        Set<Order> openOrders = Sets.newHashSet();
        Set<Order> completedOrders = Sets.newHashSet();
        
        while(true) {
            while (openOrders.size() < 100) {
                Order newOrder = new Order();
                String newOrderSingleFIX = newOrder.newOrderSingleFIX();
                producer.send(new ProducerRecord<String, String>(args[1], newOrderSingleFIX));
                openOrders.add(newOrder);
            }
            
            for (Order order : openOrders) {
                if (!order.isComplete()) {
                    if (order.hasFurtherExecuted()) {
                        String executionReportFIX = order.nextExecutionReportFIX();
                        producer.send(new ProducerRecord<String, String>(args[1], executionReportFIX));
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
            
            Thread.sleep(1);
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
        
        public boolean hasFurtherExecuted() { return new Random().nextInt(1000) == 0; }
        
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
