/*
 * Copyright (c) 2015-2019, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static org.junit.Assert.assertEquals;

public class TestSparkMLDeriver {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testDeriveWithExplicitStep() throws Exception {
    String modelPath = folder.newFolder().getAbsolutePath();
    generateAndSaveModel(modelPath);

    Properties configProps = new Properties();
    configProps.setProperty(SparkMLDeriver.DEPENDENCY_STEP_NAME_CONFIG, "step1");
    configProps.setProperty(SparkMLDeriver.MODEL_PATH_CONFIG, modelPath);
    Config config = ConfigFactory.parseProperties(configProps);

    SparkMLDeriver deriver = new SparkMLDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    derive(deriver, getDependencies());
  }

  @Test
  public void testDeriveWithImplicitStep() throws Exception {
    String modelPath = folder.newFolder().getAbsolutePath();
    generateAndSaveModel(modelPath);

    Properties configProps = new Properties();
    configProps.setProperty(SparkMLDeriver.MODEL_PATH_CONFIG, modelPath);
    Config config = ConfigFactory.parseProperties(configProps);

    SparkMLDeriver deriver = new SparkMLDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    derive(deriver, getDependencies());
  }

  @Test (expected = RuntimeException.class)
  public void testFailOnZeroDependencies() throws Exception {
    String modelPath = folder.newFolder().getAbsolutePath();
    generateAndSaveModel(modelPath);

    Properties configProps = new Properties();
    configProps.setProperty(SparkMLDeriver.MODEL_PATH_CONFIG, modelPath);
    Config config = ConfigFactory.parseProperties(configProps);

    SparkMLDeriver deriver = new SparkMLDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    derive(deriver, Maps.<String, Dataset<Row>>newHashMap());
  }

  @Test (expected = RuntimeException.class)
  public void testFailOnMultipleDependencies() throws Exception {
    String modelPath = folder.newFolder().getAbsolutePath();
    generateAndSaveModel(modelPath);

    Properties configProps = new Properties();
    configProps.setProperty(SparkMLDeriver.MODEL_PATH_CONFIG, modelPath);
    Config config = ConfigFactory.parseProperties(configProps);

    SparkMLDeriver deriver = new SparkMLDeriver();
    assertNoValidationFailures(deriver, config);
    deriver.configure(config);

    Map<String, Dataset<Row>> dependencies = getDependencies();
    dependencies.put("step2", dependencies.get("step1"));

    derive(deriver, dependencies);
  }

  private Map<String, Dataset<Row>> getDependencies() {
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    Dataset<Row> input = Contexts.getSparkSession().createDataFrame(
        Lists.newArrayList(
            RowFactory.create("spark f g h"),
            RowFactory.create("hello world")
        ),
        DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("text", DataTypes.StringType, false)
        ))
    );
    dependencies.put("step1", input);

    return dependencies;
  }

  private void derive(Deriver deriver, Map<String, Dataset<Row>> dependencies) throws Exception {
    List<Row> derived = deriver.derive(dependencies).collectAsList();

    assertEquals(1.0, derived.get(0).getAs("prediction"));
    assertEquals(0.0, derived.get(1).getAs("prediction"));
  }

  private void generateAndSaveModel(String savePath) throws IOException {
    // Sourced from the Spark ML documentation and examples

    StructType trainingSchema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("text", DataTypes.StringType, false),
        DataTypes.createStructField("label", DataTypes.DoubleType, false)
    ));
    Dataset<Row> training = Contexts.getSparkSession().createDataFrame(Lists.newArrayList(
        RowFactory.create(0L, "a b c d e spark", 1.0),
        RowFactory.create(1L, "b d", 0.0),
        RowFactory.create(2L, "spark f g h", 1.0),
        RowFactory.create(3L, "hadoop mapreduce", 0.0)
    ), trainingSchema);

    Tokenizer tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words");
    HashingTF hashingTF = new HashingTF()
        .setNumFeatures(1000)
        .setInputCol(tokenizer.getOutputCol())
        .setOutputCol("features");
    LogisticRegression lr = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.001);

    Pipeline pipeline = new Pipeline()
        .setStages(new PipelineStage[] {tokenizer, hashingTF, lr});

    PipelineModel model = pipeline.fit(training);

    model.write().overwrite().save(savePath);
  }

}
