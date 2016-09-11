package com.cloudera.labs.envelope.plan.bulk;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import com.cloudera.labs.envelope.plan.MutationType;
import com.typesafe.config.ConfigFactory;

import scala.Tuple2;

public class TestInsertOverwritePlanner {
    
    @Test
    public void testPlanner() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[1]");
        conf.setAppName("TestInsertOverwritePlanner.testPlanner");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlc = new SQLContext(jsc);
        
        DataFrame testData = sqlc.sql("SELECT 'test'");
        
        BulkWritePlanner planner = new InsertOverwritePlanner(ConfigFactory.empty());
        List<Tuple2<MutationType, DataFrame>> plan = planner.planMutationsForSet(testData);
        
        assertEquals(plan.size(), 1);
        assertEquals(plan.get(0)._1(), MutationType.OVERWRITE);
        assertEquals(plan.get(0)._2(), testData);
        
        jsc.close();
    }
    
}
