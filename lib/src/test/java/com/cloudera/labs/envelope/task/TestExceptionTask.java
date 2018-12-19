package com.cloudera.labs.envelope.task;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.Properties;

public class TestExceptionTask {

  @Test (expected = RuntimeException.class)
  public void testException() {
    Properties configProps = new Properties();
    configProps.setProperty(ExceptionTask.MESSAGE_CONFIG, "I meant this!");
    Config config = ConfigFactory.parseProperties(configProps);

    Task task = new ExceptionTask();
    task.configure(config);

    task.run(Maps.<String, Dataset<Row>>newHashMap());
  }

}
