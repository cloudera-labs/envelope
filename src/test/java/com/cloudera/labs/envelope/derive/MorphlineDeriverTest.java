package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.input.translate.MorphlineTranslator;
import com.cloudera.labs.envelope.input.translate.MorphlineTranslatorTest;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.MorphlineUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kitesdk.morphline.api.MorphlineCompilationException;

/**
 *
 */
@RunWith(JMockit.class)
public class MorphlineDeriverTest {

  private static final String MORPHLINE_FILE = "/morphline.conf";

  private static JavaSparkContext javaSparkContext;

  private String getResourcePath(String resource) {
    return MorphlineTranslatorTest.class.getResource(resource).getPath();
  }

  @Test
  public void getSchema(
      final @Mocked Config config
  ) throws Exception {

    new Expectations() {{
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("bar", "foo");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "string");
    }};

    // Relies on RowUtils.structTypeFor()
    MorphlineDeriver deriver = new MorphlineDeriver();
    deriver.configure(config);
    StructType schema = deriver.getSchema();

    assertEquals("Invalid number of SchemaFields", 2, schema.fields().length);
    assertEquals("Invalid DataType", DataTypes.IntegerType, schema.fields()[0].dataType());
    assertEquals("Invalid DataType", DataTypes.StringType, schema.fields()[1].dataType());
  }

  @Test (expected = RuntimeException.class)
  public void getSchemaInvalidDataType(
      final @Mocked Config config
  ) throws Exception {

    new Expectations() {{
      config.getString(MorphlineTranslator.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("bar", "foo");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int", "boom");
    }};

    // Relies on RowUtils.structTypeFor()
    Deriver deriver = new MorphlineDeriver();
    deriver.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void deriveEmptyMorphlineConfig(
      final @Mocked Config config
  ) throws Exception {

    new Expectations() {{
      config.getString(MorphlineDeriver.MORPHLINE); result = null;
    }};

    Deriver deriver = new MorphlineDeriver();
    deriver.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void deriveBlankMorphlineConfig(
      final @Mocked Config config
  ) throws Exception {

    new Expectations() {{
      config.getString(MorphlineDeriver.MORPHLINE); result = "";
    }};

    Deriver deriver = new MorphlineDeriver();
    deriver.configure(config);
  }

  @Test (expected = RuntimeException.class)
  public void deriveNoDependencies() throws Exception {
    Map<String, DataFrame> dependencies = Maps.newHashMap();

    Deriver deriver = new MorphlineDeriver();
    deriver.derive(dependencies);
  }

  @Test (expected = RuntimeException.class)
  public void deriveMultipleDependencies() throws Exception {
    Map<String, DataFrame> dependencies = Maps.newHashMap();
    dependencies.put("dep1", null);
    dependencies.put("dep2", null);

    Deriver deriver = new MorphlineDeriver();
    deriver.derive(dependencies);
  }

  @Test (expected = RuntimeException.class)
  public void deriveMorphlineMapperFunctionError(
      final @Mocked Config config,
      final @Mocked MorphlineUtils utils
  ) throws Exception {

    new MockUp<Contexts>() {
      {
        SparkConf config = new SparkConf();
        config.setAppName("Morphline mapper error");
        config.setMaster("local[*]");
        javaSparkContext = new JavaSparkContext(config);
      }

      @Mock
      public SQLContext getSQLContext() {
        return new SQLContext(javaSparkContext);
      }
      @Mock
      public JavaSparkContext getJavaSparkContext() {
        return javaSparkContext;
      }
    };

    new Expectations() {{
      config.getString(MorphlineDeriver.MORPHLINE); result = "doesn't matter";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("bar");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("int");
      MorphlineUtils.morphlineMapper(anyString, anyString, (StructType) any); result =
          new MorphlineCompilationException("Compile exception", config);
    }};

    DataFrame dataFrame = Contexts.getSQLContext().createDataFrame(
        Contexts.getJavaSparkContext().parallelize(Lists.newArrayList(RowFactory.create(1))),
        DataTypes.createStructType(Lists.newArrayList(DataTypes.createStructField("baz", DataTypes.IntegerType, false)))
    );

    Map<String, DataFrame> dependencies = Maps.newHashMap();
    dependencies.put("dep1", dataFrame);

    Deriver deriver = new MorphlineDeriver();
    deriver.configure(config);

    try {
      deriver.derive(dependencies);
    } finally {
      javaSparkContext.stop();
    }
  }

  @Test
  public void deriveIntegrationTest(
      final @Mocked Config config
  ) throws Exception {

    new MockUp<Contexts>() {
      {
        SparkConf config = new SparkConf();
        config.setAppName("Morphline mapper error");
        config.setMaster("local[*]");
        javaSparkContext = new JavaSparkContext(config);
      }

      @Mock
      public SQLContext getSQLContext() {
        return new SQLContext(javaSparkContext);
      }
      @Mock
      public JavaSparkContext getJavaSparkContext() {
        return javaSparkContext;
      }
    };

    new Expectations() {{
      config.getString(MorphlineDeriver.MORPHLINE); result = getResourcePath(MORPHLINE_FILE);
      config.getString(MorphlineDeriver.MORPHLINE_ID); result = "deriver";
      config.getStringList(MorphlineTranslator.FIELD_NAMES); result = Lists.newArrayList("foo", "bar", "baz");
      config.getStringList(MorphlineTranslator.FIELD_TYPES); result = Lists.newArrayList("string", "int", "int");
    }};

    DataFrame dataFrame = Contexts.getSQLContext().createDataFrame(
        Contexts.getJavaSparkContext().parallelize(Lists.newArrayList(RowFactory.create(987, "string value"))),
        DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("one", DataTypes.IntegerType, false),
            DataTypes.createStructField("two", DataTypes.StringType, false))
        )
    );

    Map<String, DataFrame> dependencies = Maps.newHashMap();
    dependencies.put("dep1", dataFrame);

    Deriver deriver = new MorphlineDeriver();
    deriver.configure(config);

    try {
      DataFrame outputDF = deriver.derive(dependencies);
      outputDF.printSchema();
      List<Row> rowList = outputDF.collectAsList();
      assertEquals(1, rowList.size());
      assertEquals(3, rowList.get(0).size());
      assertTrue(rowList.get(0).get(0) instanceof String);
      assertTrue(rowList.get(0).get(1) instanceof Integer);
      assertTrue(rowList.get(0).get(2) instanceof Integer);
    } finally {
      javaSparkContext.stop();
    }
  }

}