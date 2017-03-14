package com.cloudera.labs.envelope.output;

import com.cloudera.labs.envelope.input.JdbcInput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.integration.junit4.JMockit;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.h2.tools.Server;
import org.junit.*;
import org.junit.runner.RunWith;
import scala.Tuple2;

import java.sql.*;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;


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

  @Before
  public void setUp() {
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

    ArrayList<Tuple2<MutationType, DataFrame>> planned = new ArrayList<>();
    SQLContext sqlContext = Contexts.getSQLContext();
    DataFrame o = sqlContext.read().json(JdbcInput.class.getResource(SAMPLE_DATA_PATH).getPath());
    Tuple2<MutationType, DataFrame> input = new Tuple2<>(MutationType.INSERT, o);

    planned.add(input);

    jdbcOutput.applyBulkMutations(planned);

    Connection connection = DriverManager.getConnection(JDBC_URL, JDBC_USERNAME, JDBC_PASSWORD);
    Statement stmt = connection.createStatement();
    ResultSet resultSet = stmt.executeQuery("select count(*) from user2");
    resultSet.next();
    assertEquals(2, resultSet.getInt(1));
  }


  @Test(expected = RuntimeException.class)
  public void checkApplyBulkMutations_Exception_TableExist() throws Exception {
    JdbcOutput jdbcOutput = new JdbcOutput();
    jdbcOutput.configure(ConfigUtils.configFromPath(JdbcInput.class.getResource(JDBC_PROPERTIES_TABLE_USER_PATH).getPath()));

    ArrayList<Tuple2<MutationType, DataFrame>> planned = new ArrayList<>();
    SQLContext sqlContext = Contexts.getSQLContext();
    DataFrame o = sqlContext.read().json(JdbcInput.class.getResource(SAMPLE_DATA_PATH).getPath());
    Tuple2<MutationType, DataFrame> input = new Tuple2<>(MutationType.INSERT, o);

    planned.add(input);

    jdbcOutput.applyBulkMutations(planned);
  }


  @Test(expected = RuntimeException.class)
  public void checkApplyBulkMutations_Exception_MutationTypeNotSupported() throws Exception {
    JdbcOutput jdbcOutput = new JdbcOutput();
    jdbcOutput.configure(ConfigUtils.configFromPath(JdbcInput.class.getResource(JDBC_PROPERTIES_TABLE_USER_PATH).getPath()));

    ArrayList<Tuple2<MutationType, DataFrame>> planned = new ArrayList<>();
    SQLContext sqlContext = Contexts.getSQLContext();
    DataFrame o = sqlContext.read().json(JdbcInput.class.getResource(SAMPLE_DATA_PATH).getPath());
    Tuple2<MutationType, DataFrame> input = new Tuple2<>(MutationType.OVERWRITE, o);

    planned.add(input);

    jdbcOutput.applyBulkMutations(planned);
  }


  @After
  public void tearDown() {
    if (sparkContext!=null) {
      sparkContext.stop();
    }
  }

  @AfterClass
  public static void afterClass() {
    server.stop();

  }


}
