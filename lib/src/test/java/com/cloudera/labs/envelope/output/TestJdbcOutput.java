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

package com.cloudera.labs.envelope.output;

import com.cloudera.labs.envelope.input.JdbcInput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.validate.ValidationAssert;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Properties;
import mockit.integration.junit4.JMockit;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

@RunWith(JMockit.class)
public class TestJdbcOutput {

  private static final String SAMPLE_DATA_PATH = "/JdbcTest/sample.json";
  private static final String JDBC_URL = "jdbc:h2:tcp://127.0.0.1:%d/mem:test;DB_CLOSE_DELAY=-1";
  private static final String JDBC_USERNAME = "sa";
  private static final String JDBC_PASSWORD = "";
  private static final String JDBC_TABLE = "user";
  private static Config config;
  private static Server server;

  @BeforeClass
  public static void beforeClass() throws SQLException, ClassNotFoundException {
    Class.forName("org.h2.Driver");
    server = Server.createTcpServer("-tcp", "-tcpAllowOthers").start();
    Connection connection = DriverManager.getConnection(String.format(JDBC_URL, server.getPort()),
        JDBC_USERNAME, JDBC_PASSWORD);
    Statement stmt = connection.createStatement();
    stmt.executeUpdate("create table if not exists user (firstname varchar(30), lastname varchar(30))");

    Properties properties = new Properties();
    properties.setProperty("url", String.format(JDBC_URL, server.getPort()));
    properties.setProperty("tablename", JDBC_TABLE);
    properties.setProperty("username", JDBC_USERNAME);
    properties.setProperty("password", JDBC_PASSWORD);

    config = ConfigFactory.parseProperties(properties);
  }

  @Test
  public void checkDB_OK() throws SQLException {
    Connection connection = DriverManager.getConnection(String.format(JDBC_URL, server.getPort()),
        JDBC_USERNAME, JDBC_PASSWORD);
    connection.createStatement();
  }

  @Test
  public void checkApplyBulkMutations_works() throws Exception {
    JdbcOutput jdbcOutput = new JdbcOutput();
    ValidationAssert.assertNoValidationFailures(jdbcOutput, config);
    jdbcOutput.configure(ConfigFactory.parseString("tablename=user2").withFallback(config));

    ArrayList<Tuple2<MutationType, Dataset<Row>>> planned = new ArrayList<>();
    Dataset<Row> o = Contexts.getSparkSession().read().json(JdbcInput.class.getResource(SAMPLE_DATA_PATH).getPath());
    Tuple2<MutationType, Dataset<Row>> input = new Tuple2<>(MutationType.INSERT, o);

    planned.add(input);

    jdbcOutput.applyBulkMutations(planned);

    Connection connection = DriverManager.getConnection(String.format(JDBC_URL, server.getPort()),
        JDBC_USERNAME, JDBC_PASSWORD);
    Statement stmt = connection.createStatement();
    ResultSet resultSet = stmt.executeQuery("select count(*) from user2");
    resultSet.next();
    assertEquals(2, resultSet.getInt(1));
  }

  @Test(expected = AnalysisException.class)
  public void checkApplyBulkMutations_Exception_TableExist() {
    JdbcOutput jdbcOutput = new JdbcOutput();
    ValidationAssert.assertNoValidationFailures(jdbcOutput, config);
    jdbcOutput.configure(config);

    ArrayList<Tuple2<MutationType, Dataset<Row>>> planned = new ArrayList<>();
    Dataset<Row> o = Contexts.getSparkSession().read().json(JdbcInput.class.getResource(SAMPLE_DATA_PATH).getPath());
    Tuple2<MutationType, Dataset<Row>> input = new Tuple2<>(MutationType.INSERT, o);

    planned.add(input);

    jdbcOutput.applyBulkMutations(planned);
  }

  @Test(expected = RuntimeException.class)
  public void checkApplyBulkMutations_Exception_MutationTypeNotSupported() {
    JdbcOutput jdbcOutput = new JdbcOutput();
    ValidationAssert.assertNoValidationFailures(jdbcOutput, config);
    jdbcOutput.configure(config);

    ArrayList<Tuple2<MutationType, Dataset<Row>>> planned = new ArrayList<>();
    Dataset<Row> o = Contexts.getSparkSession().read().json(JdbcInput.class.getResource(SAMPLE_DATA_PATH).getPath());
    Tuple2<MutationType, Dataset<Row>> input = new Tuple2<>(MutationType.OVERWRITE, o);

    planned.add(input);

    jdbcOutput.applyBulkMutations(planned);
  }

  @AfterClass
  public static void afterClass() {
    server.stop();
  }

}
