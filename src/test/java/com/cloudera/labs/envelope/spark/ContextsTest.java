package com.cloudera.labs.envelope.spark;

import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.spark.SparkConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ContextsTest {

    private static final String RESOURCES_PATH = "/spark";

    @Test
    public void testSparkPassthroughGood() {
        Config config = ConfigUtils.configFromPath(
            this.getClass().getResource(RESOURCES_PATH + "/spark-passthrough-good.conf").getPath());

        SparkConf sparkConf = Contexts.getSparkConfiguration(config);

        assertTrue(sparkConf.contains("spark.driver.allowMultipleContexts"));
        assertEquals("true", sparkConf.get("spark.driver.allowMultipleContexts"));

        assertTrue(sparkConf.contains("spark.master"));
        assertEquals("local[1]", sparkConf.get("spark.master"));
    }

    @Test
    public void testSparkPassthroughWithInvalid() {
        Config config = ConfigUtils.configFromPath(
            this.getClass().getResource(RESOURCES_PATH + "/spark-passthrough-with-invalid.conf").getPath());

        SparkConf sparkConf = Contexts.getSparkConfiguration(config);

        assertTrue(sparkConf.contains("spark.driver.allowMultipleContexts"));
        assertEquals("true", sparkConf.get("spark.driver.allowMultipleContexts"));

        assertTrue(sparkConf.contains("spark.master"));
        assertEquals("local[1]", sparkConf.get("spark.master"));

        assertFalse(sparkConf.contains("spark.invalid.conf"));
    }

}
