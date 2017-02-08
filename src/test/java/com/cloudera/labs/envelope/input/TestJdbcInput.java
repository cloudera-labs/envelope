package com.cloudera.labs.envelope.input;

import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.integration.junit4.JMockit;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.*;

import static org.junit.Assert.*;

import org.h2.tools.Server;
import org.junit.runner.RunWith;

import java.sql.*;

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

        new MockUp<Contexts>() {
            @Mock
            public SQLContext getSQLContext() {
                SparkConf config = new SparkConf();
                config.setAppName("JDBC test");
                config.setMaster("local[1]");
                sparkContext = new SparkContext(config);
                return new SQLContext(sparkContext);
            }

        };
        JdbcInput jdbcInput = new JdbcInput();
        jdbcInput.configure(ConfigUtils.configFromPath(JdbcInput.class.getResource(JDBC_PROPERTIES_PATH).getPath()));
        DataFrame read = jdbcInput.read();
        assertNotNull(read);
        assertEquals(3, read.count());
        assertEquals(2, read.schema().size());
        Row[] rows = read.collect();
        for (Row row : rows) {
            assertEquals(2, row.size());
            assertNotNull(row.get(0));
            assertNotNull(row.get(1));
        }
        sparkContext.stop();
    }

    @AfterClass
    public static void afterClass() {
        server.stop();

    }
}
