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

package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.cloudera.labs.envelope.validate.ValidationAssert.assertNoValidationFailures;
import static com.cloudera.labs.envelope.validate.ValidationAssert.assertValidationFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
*  Test class for Select deriver
*/
public class TestSelectDeriver {

  @Test
  public void normalSelectDerive() throws Exception {
    List<String> includeFields = Arrays.asList("sourceid", "sourcename", "sourcestatus", "country", "destid", "destsystem");
    List<String> excludeFields = Arrays.asList("loadtype", "loaddate", "loadtime", "loadsize", "producttype");

    Dataset<Row> inputdataframe = testDataframe();
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dataSource1", inputdataframe);
    SelectDeriver selectderiver = new SelectDeriver();
    
    // Test include-fields of columns
    Config config = ConfigFactory.empty()
           .withValue(SelectDeriver.INCLUDE_FIELDS,
    	    ConfigValueFactory.fromAnyRef(includeFields));
    assertNoValidationFailures(selectderiver, config);
    selectderiver.configure(config);
    List<String> rescolumns = Arrays.asList(selectderiver.derive(dependencies).columns());
    assertEquals(6, rescolumns.size());
    assertTrue(rescolumns.containsAll(includeFields));

    // Test exclude-fields of columns    
    config = ConfigFactory.empty()
           .withValue(SelectDeriver.EXCLUDE_FIELDS,
           ConfigValueFactory.fromAnyRef(excludeFields));
    assertNoValidationFailures(selectderiver, config);
    selectderiver.configure(config);
    rescolumns = Arrays.asList(selectderiver.derive(dependencies).columns());
    assertEquals(6, rescolumns.size());
    assertTrue(!rescolumns.containsAll(excludeFields));

    // Test include-fields of columns with multiples dependencies
    config = ConfigFactory.empty()
           .withValue(SelectDeriver.INCLUDE_FIELDS,
    	   ConfigValueFactory.fromAnyRef(includeFields))
           .withValue(SelectDeriver.STEP_NAME_CONFIG,
           ConfigValueFactory.fromAnyRef("dataSource1"));
    dependencies.put("dataSource2", null);
    dependencies.put("dataSource3", null);
    assertNoValidationFailures(selectderiver, config);
    selectderiver.configure(config);
    rescolumns = Arrays.asList(selectderiver.derive(dependencies).columns());
    assertEquals(rescolumns.size(), 6);
    assertTrue(rescolumns.containsAll(includeFields));
}

  // Test if dependencies are missing 
  @Test(expected = RuntimeException.class)
  public void missingDependencies() throws Exception {
    List<String> includeFields = Arrays.asList("sourceid", "sourcename", "sourcestatus");
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    Config config = ConfigFactory.empty()
           .withValue(SelectDeriver.INCLUDE_FIELDS,ConfigValueFactory.fromAnyRef(includeFields))
           .withValue(SelectDeriver.STEP_NAME_CONFIG,ConfigValueFactory.fromAnyRef("dataSource1"));
    SelectDeriver selectderiver = new SelectDeriver();
    assertNoValidationFailures(selectderiver, config);
    selectderiver.configure(config);
    selectderiver.derive(dependencies);
  }

  // Test if configurations are missing   
  @Test
  public void missingConfig() throws Exception {
    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dataSource1", null);
    dependencies.put("dataSource2", null);
    Config config = ConfigFactory.empty();
    SelectDeriver selectderiver = new SelectDeriver();
    assertValidationFailures(selectderiver, config);
  }
 
  //Test if both include-fields and exclude-fields are provided   
  @Test
  public void wrongConfigBothListProvided() throws Exception {
    List<String> includeFields = Arrays.asList("sourceid", "sourcename", "sourcestatus");
    List<String> excludeFields = Arrays.asList("loadtype", "loaddate", "loadtime", "loadsize", "producttype");

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dataSource1", null);
    dependencies.put("dataSource2", null);
    
    Config config = ConfigFactory.empty()
            .withValue(SelectDeriver.INCLUDE_FIELDS,ConfigValueFactory.fromAnyRef(includeFields))
            .withValue(SelectDeriver.EXCLUDE_FIELDS,ConfigValueFactory.fromAnyRef(excludeFields))
            .withValue(SelectDeriver.STEP_NAME_CONFIG,ConfigValueFactory.fromAnyRef("dataSource1"));
    SelectDeriver selectderiver = new SelectDeriver();
    assertValidationFailures(selectderiver, config);
  }

  // Test if wrong step name was passed    
  @Test(expected = RuntimeException.class)
  public void wrongStepNameConfig() throws Exception {
    List<String> includeFields = Arrays.asList("sourceid", "sourcename", "sourcestatus");

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    dependencies.put("dataSource1", null);
    dependencies.put("dataSource2", null);

     Config config = ConfigFactory.empty()
            .withValue(SelectDeriver.INCLUDE_FIELDS,ConfigValueFactory.fromAnyRef(includeFields))
            .withValue(SelectDeriver.STEP_NAME_CONFIG,ConfigValueFactory.fromAnyRef("dataSource5"));
    SelectDeriver selectderiver = new SelectDeriver();
    assertNoValidationFailures(selectderiver, config);
    selectderiver.configure(config);
    selectderiver.derive(dependencies);   
  }
    
  // Test if there is unknown column specified in include-fields  
  @Test(expected = RuntimeException.class)
  public void wrongConfigUnknownColumnIncludeFields() throws Exception {
    List<String> includeFields = Arrays.asList("sourceid", "sourcename", "sourcestatus", "TestField");

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    Dataset<Row> inputdataframe = testDataframe();
    dependencies.put("dataSource1", inputdataframe); 
    Config config = ConfigFactory.empty()
            .withValue(SelectDeriver.INCLUDE_FIELDS,ConfigValueFactory.fromAnyRef(includeFields))
            .withValue(SelectDeriver.STEP_NAME_CONFIG,ConfigValueFactory.fromAnyRef("dataSource1"));
    SelectDeriver selectderiver = new SelectDeriver();
    assertNoValidationFailures(selectderiver, config);
    selectderiver.configure(config);
    selectderiver.derive(dependencies);  
  }
  
  // Test if there is unknown column specified in exclude fields  
  @Test(expected = RuntimeException.class)
  public void wrongConfigUnknownColumnExcludeFields() throws Exception {
    List<String> excludeFields = Arrays.asList("loadtype", "loaddate", "loadtime", "TestField");

    Map<String, Dataset<Row>> dependencies = Maps.newHashMap();
    Dataset<Row> inputdataframe = testDataframe();
    dependencies.put("dataSource1", inputdataframe); 
    Config config = ConfigFactory.empty()
            .withValue(SelectDeriver.EXCLUDE_FIELDS,ConfigValueFactory.fromAnyRef(excludeFields))
            .withValue(SelectDeriver.STEP_NAME_CONFIG,ConfigValueFactory.fromAnyRef("dataSource1"));
    SelectDeriver selectderiver = new SelectDeriver();
    assertNoValidationFailures(selectderiver, config);
    selectderiver.configure(config);
    selectderiver.derive(dependencies);
  }

  private static Dataset<Row> testDataframe() {
    // Adding 11 fields for test schema 
	StructType schema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("sourceid", DataTypes.IntegerType, true),
        DataTypes.createStructField("sourcename", DataTypes.StringType, true),
        DataTypes.createStructField("sourcestatus", DataTypes.StringType, true),
        DataTypes.createStructField("country", DataTypes.StringType, true),
        DataTypes.createStructField("loadtype", DataTypes.StringType, true),
        DataTypes.createStructField("loaddate", DataTypes.StringType, true),
        DataTypes.createStructField("loadtime", DataTypes.StringType, true),
        DataTypes.createStructField("loadsize", DataTypes.IntegerType, true),
        DataTypes.createStructField("producttype", DataTypes.StringType, true), 
        DataTypes.createStructField("destid", DataTypes.IntegerType, true),
        DataTypes.createStructField("destsystem", DataTypes.StringType, true)));
    // Adding 1 row for test data
    List<Row> rows = Lists.newArrayList(
        RowFactory.create(1, "NYSE", "Active", "USA", "hourly", "2018-04-27", "17:00:00", 40000, "trades", 23, "kafka"));
    return Contexts.getSparkSession().createDataFrame(rows, schema);
  }

}