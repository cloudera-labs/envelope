package com.cloudera.labs.envelope.derive;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestNestDeriver {
    
    private static SQLContext sqlc;
    
    @BeforeClass
    public static void beforeClass() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[1]");
        conf.setAppName("TestNestDeriver");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        sqlc = new SQLContext(jsc);
    }
    
    @AfterClass
    public static void afterClass() {
        sqlc.sparkContext().stop();
    }
    
    @Test
    public void testOneKeyFieldName() throws Exception {
        StructType ordersSchema = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("order_id", DataTypes.IntegerType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("customer_id", DataTypes.IntegerType, true)));
        
        StructType customersSchema = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("customer_id", DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true)));
        
        List<Row> orderRows = Lists.newArrayList();
        orderRows.add(RowFactory.create(1000, "Envelopes", 10000));
        orderRows.add(RowFactory.create(1001, "Stamps", 10000));
        orderRows.add(RowFactory.create(1002, "Pens", 10000));
        orderRows.add(RowFactory.create(1003, "Paper", 10001));
        
        List<Row> customerRows = Lists.newArrayList();
        customerRows.add(RowFactory.create(10000, "Jane"));
        customerRows.add(RowFactory.create(10001, "Joe"));
        
        DataFrame orders = sqlc.createDataFrame(orderRows, ordersSchema);
        DataFrame customers = sqlc.createDataFrame(customerRows, customersSchema);
        
        Map<String, DataFrame> dependencies = Maps.newHashMap();
        dependencies.put("orders", orders);
        dependencies.put("customers", customers);
        
        Map<String, Object> configMap = Maps.newHashMap();
        configMap.put(NestDeriver.NEST_FROM_CONFIG_NAME, "orders");
        configMap.put(NestDeriver.NEST_INTO_CONFIG_NAME, "customers");
        configMap.put(NestDeriver.KEY_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("customer_id"));
        configMap.put(NestDeriver.NESTED_FIELD_NAME_CONFIG_NAME, "orders");
        Config config = ConfigFactory.parseMap(configMap);
        
        Deriver deriver = new NestDeriver();
        deriver.configure(config);
        
        DataFrame nested = deriver.derive(dependencies);
        
        assertEquals(nested.count(), 2);
        
        List<Row> jane = nested.where("name = 'Jane'").collectAsList();
        assertEquals(jane.size(), 1);
        Row janeRow = jane.get(0);
        assertEquals(janeRow.getList(janeRow.fieldIndex("orders")).size(), 3);
        
        List<Row> joe = nested.where("name = 'Joe'").collectAsList();
        assertEquals(joe.size(), 1);
        Row joeRow = joe.get(0);
        assertEquals(joeRow.getList(joeRow.fieldIndex("orders")).size(), 1);
    }
    
    @Test
    public void testMultipleKeyFieldNames() throws Exception {
        StructType ordersSchema = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("order_id", DataTypes.IntegerType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("customer_first", DataTypes.StringType, true),
                DataTypes.createStructField("customer_last", DataTypes.StringType, true)));
        
        StructType customersSchema = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("customer_first", DataTypes.StringType, true),
                DataTypes.createStructField("customer_last", DataTypes.StringType, true),
                DataTypes.createStructField("state", DataTypes.StringType, true)));
        
        List<Row> orderRows = Lists.newArrayList();
        orderRows.add(RowFactory.create(1000, "Envelopes", "Jane", "Smith"));
        orderRows.add(RowFactory.create(1001, "Stamps", "Jane", "Smith"));
        orderRows.add(RowFactory.create(1002, "Pens", "Jane", "Smith"));
        orderRows.add(RowFactory.create(1003, "Paper", "Jane", "Bloggs"));
        
        List<Row> customerRows = Lists.newArrayList();
        customerRows.add(RowFactory.create("Jane", "Smith", "NY"));
        customerRows.add(RowFactory.create("Jane", "Bloggs", "CA"));
        
        DataFrame orders = sqlc.createDataFrame(orderRows, ordersSchema);
        DataFrame customers = sqlc.createDataFrame(customerRows, customersSchema);
        
        Map<String, DataFrame> dependencies = Maps.newHashMap();
        dependencies.put("orders", orders);
        dependencies.put("customers", customers);
        
        Map<String, Object> configMap = Maps.newHashMap();
        configMap.put(NestDeriver.NEST_FROM_CONFIG_NAME, "orders");
        configMap.put(NestDeriver.NEST_INTO_CONFIG_NAME, "customers");
        configMap.put(NestDeriver.KEY_FIELD_NAMES_CONFIG_NAME, Lists.newArrayList("customer_first", "customer_last"));
        configMap.put(NestDeriver.NESTED_FIELD_NAME_CONFIG_NAME, "orders");
        Config config = ConfigFactory.parseMap(configMap);
        
        Deriver deriver = new NestDeriver();
        deriver.configure(config);
        
        DataFrame nested = deriver.derive(dependencies);
        
        assertEquals(nested.count(), 2);
        
        List<Row> smith = nested.where("customer_first = 'Jane' AND customer_last = 'Smith'").collectAsList();
        assertEquals(smith.size(), 1);
        Row smithRow = smith.get(0);
        assertEquals(smithRow.getList(smithRow.fieldIndex("orders")).size(), 3);
        
        List<Row> bloggs = nested.where("customer_first = 'Jane' AND customer_last = 'Bloggs'").collectAsList();
        assertEquals(bloggs.size(), 1);
        Row bloggsRow = bloggs.get(0);
        assertEquals(bloggsRow.getList(bloggsRow.fieldIndex("orders")).size(), 1);
    }
    
}
