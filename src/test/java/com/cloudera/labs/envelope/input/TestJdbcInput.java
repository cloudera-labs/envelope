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
package com.cloudera.labs.envelope.input;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.cloudera.labs.envelope.utils.ConfigUtils;

import mockit.integration.junit4.JMockit;

/**
 * Test h2 table user with two columns firstname, lastname and three rows using JdbcInput
 */

@RunWith(JMockit.class)
public class TestJdbcInput {


  public static final String JDBC_PROPERTIES_PATH = "/JdbcTest/jdbc-table-user.properties";
  public static Server server;
  public static SparkContext sparkContext;

  @BeforeClass
  public static void beforeClass() throws SQLException, ClassNotFoundException, InterruptedException {
    Class.forName("org.h2.Driver");
    server = Server.createTcpServer("-tcp", "-tcpAllowOthers", "-tcpPort", "9092").start();
    Connection connection = DriverManager.getConnection("jdbc:h2:tcp://127.0.0.1:9092/mem:test;DB_CLOSE_DELAY=-1", "sa", "");
    Statement stmt = connection.createStatement();
    stmt.executeUpdate("create table if not exists user (firstname varchar(30), lastname varchar(30))");
    stmt.executeUpdate("insert into user values ('f1','p1')");
    stmt.executeUpdate("insert into user values ('f2','p1')");
    stmt.executeUpdate("insert into user values ('f3','p1')");
  }


  @Test
  public void checkDB_OK() throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:h2:tcp://127.0.0.1:9092/mem:test;DB_CLOSE_DELAY=-1", "sa", "");
    Statement stmt = connection.createStatement();
    ResultSet resultSet = stmt.executeQuery("select count(*) from user");
    resultSet.next();
    assertEquals(3, resultSet.getInt(1));
  }


  @Test
  public void checkJdbcInput_works() throws Exception {
    JdbcInput jdbcInput = new JdbcInput();
    jdbcInput.configure(ConfigUtils.configFromPath(JdbcInput.class.getResource(JDBC_PROPERTIES_PATH).getPath()));
    Dataset<Row> read = jdbcInput.read();
    assertNotNull(read);
    assertEquals(3, read.count());
    assertEquals(2, read.schema().size());
    List<Row> rows = read.collectAsList();
    for (Row row : rows) {
      assertEquals(2, row.size());
      assertNotNull(row.get(0));
      assertNotNull(row.get(1));
    }
  }

  @AfterClass
  public static void afterClass() {
    server.stop();
  }
}
