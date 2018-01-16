/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.output;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.cloudera.labs.envelope.input.JdbcInput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;

import mockit.integration.junit4.JMockit;
import scala.Tuple2;


@RunWith(JMockit.class)
public class TestJdbcOutput {


  public static final String JDBC_PROPERTIES_TABLE_USER_PATH = "/JdbcTest/jdbc-table-user.properties";
  public static final String JDBC_PROPERTIES_TABLE_USER2_PATH = "/JdbcTest/jdbc-table-user2.properties";

  public static final String SAMPLE_DATA_PATH = "/JdbcTest/sample.json";
  public static final String JDBC_URL = "jdbc:h2:tcp://127.0.0.1:9092/mem:test;DB_CLOSE_DELAY=-1";
  public static final String JDBC_USERNAME = "sa";
  public static final String JDBC_PASSWORD = "";
  public static Server server;
  public static SparkContext sparkContext;

  @BeforeClass
  public static void beforeClass() throws SQLException, ClassNotFoundException, InterruptedException {
    Class.forName("org.h2.Driver");
    server = Server.createTcpServer("-tcp", "-tcpAllowOthers", "-tcpPort", "9092").start();
    Connection connection = DriverManager.getConnection(JDBC_URL, JDBC_USERNAME, JDBC_PASSWORD);
    Statement stmt = connection.createStatement();
    stmt.executeUpdate("create table if not exists user (firstname varchar(30), lastname varchar(30))");

  }

  @Test
  public void checkDB_OK() throws SQLException {
    Connection connection = DriverManager.getConnection(JDBC_URL, JDBC_USERNAME, JDBC_PASSWORD);
    connection.createStatement();
  }


  @Test
  public void checkApplyBulkMutations_works() throws Exception {
    JdbcOutput jdbcOutput = new JdbcOutput();
    jdbcOutput.configure(ConfigUtils.configFromPath(JdbcInput.class.getResource(JDBC_PROPERTIES_TABLE_USER2_PATH).getPath()));

    ArrayList<Tuple2<MutationType, Dataset<Row>>> planned = new ArrayList<>();
    Dataset<Row> o = Contexts.getSparkSession().read().json(JdbcInput.class.getResource(SAMPLE_DATA_PATH).getPath());
    Tuple2<MutationType, Dataset<Row>> input = new Tuple2<>(MutationType.INSERT, o);

    planned.add(input);

    jdbcOutput.applyBulkMutations(planned);

    Connection connection = DriverManager.getConnection(JDBC_URL, JDBC_USERNAME, JDBC_PASSWORD);
    Statement stmt = connection.createStatement();
    ResultSet resultSet = stmt.executeQuery("select count(*) from user2");
    resultSet.next();
    assertEquals(2, resultSet.getInt(1));
  }


  @Test(expected = AnalysisException.class)
  public void checkApplyBulkMutations_Exception_TableExist() throws Exception {
    JdbcOutput jdbcOutput = new JdbcOutput();
    jdbcOutput.configure(ConfigUtils.configFromPath(JdbcInput.class.getResource(JDBC_PROPERTIES_TABLE_USER_PATH).getPath()));

    ArrayList<Tuple2<MutationType, Dataset<Row>>> planned = new ArrayList<>();
    Dataset<Row> o = Contexts.getSparkSession().read().json(JdbcInput.class.getResource(SAMPLE_DATA_PATH).getPath());
    Tuple2<MutationType, Dataset<Row>> input = new Tuple2<>(MutationType.INSERT, o);

    planned.add(input);

    jdbcOutput.applyBulkMutations(planned);
  }


  @Test(expected = RuntimeException.class)
  public void checkApplyBulkMutations_Exception_MutationTypeNotSupported() throws Exception {
    JdbcOutput jdbcOutput = new JdbcOutput();
    jdbcOutput.configure(ConfigUtils.configFromPath(JdbcInput.class.getResource(JDBC_PROPERTIES_TABLE_USER_PATH).getPath()));

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
