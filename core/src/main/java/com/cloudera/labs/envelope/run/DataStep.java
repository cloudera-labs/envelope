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

package com.cloudera.labs.envelope.run;

import com.cloudera.labs.envelope.derive.Deriver;
import com.cloudera.labs.envelope.derive.DeriverFactory;
import com.cloudera.labs.envelope.input.Input;
import com.cloudera.labs.envelope.input.InputFactory;
import com.cloudera.labs.envelope.output.BulkOutput;
import com.cloudera.labs.envelope.output.Output;
import com.cloudera.labs.envelope.output.OutputFactory;
import com.cloudera.labs.envelope.output.RandomOutput;
import com.cloudera.labs.envelope.partition.PartitionerFactory;
import com.cloudera.labs.envelope.plan.BulkPlanner;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.Planner;
import com.cloudera.labs.envelope.plan.PlannerFactory;
import com.cloudera.labs.envelope.plan.RandomPlanner;
import com.cloudera.labs.envelope.spark.AccumulatorRequest;
import com.cloudera.labs.envelope.spark.Accumulators;
import com.cloudera.labs.envelope.spark.UsesAccumulators;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A data step is a step that will contain a DataFrame that other steps can use.
 * The DataFrame can be created either by an input or a deriver.
 * The DataFrame can be optionally written to an output, as planned by the planner.
 */
public abstract class DataStep
    extends Step implements UsesAccumulators, ProvidesValidations, InstantiatesComponents {

  public static final String INPUT_TYPE = "input";
  public static final String DERIVER_TYPE = "deriver";
  public static final String PLANNER_TYPE = "planner";
  public static final String OUTPUT_TYPE = "output";
  public static final String PARTITIONER_TYPE = "partitioner";
  public static final String CACHE_ENABLED_PROPERTY = "cache.enabled";
  public static final String CACHE_STORAGE_LEVEL_PROPERTY = "cache.storage.level";
  public static final String SMALL_HINT_PROPERTY = "hint.small";
  public static final String PRINT_SCHEMA_ENABLED_PROPERTY = "print.schema.enabled";
  public static final String PRINT_DATA_ENABLED_PROPERTY = "print.data.enabled";
  public static final String PRINT_DATA_LIMIT_PROPERTY = "print.data.limit";
  
  private static final String ACCUMULATOR_SECONDS_EXTRACTING_KEYS = "Seconds spent extracting keys";
  private static final String ACCUMULATOR_SECONDS_EXISTING = "Seconds spent getting existing";
  private static final String ACCUMULATOR_SECONDS_PLANNING = "Seconds spent random planning";
  private static final String ACCUMULATOR_SECONDS_APPLYING = "Seconds spent applying random mutations";

  private Dataset<Row> data;
  private Input input;
  private Deriver deriver;
  private Planner planner;
  private Output output;
  private Accumulators accumulators;

  public DataStep(String name) {
    super(name);
  }

  public Dataset<Row> getData() {
    return data;  
  }

  public void setData(Dataset<Row> batchDF) {
    this.data = batchDF;

    if (doesCache()) {
      cache();
    }

    if (usesSmallHint()) {
      applySmallHint();
    }
    
    if (doesPrintSchema()) {
      printSchema();
    }
    
    if (doesPrintData()) {
      printData();
    }

    registerStep();
    
    if (hasOutput()) {
      writeOutput();
    }
  }

  @Override
  public void reset() {
    if (hasSubmitted()) {
      clearCache();
      setSubmitted(false);
    }
  }

  public boolean hasInput() {
    return config.hasPath(INPUT_TYPE);
  }

  public boolean hasDeriver() {
    return config.hasPath(DERIVER_TYPE);
  }

  public boolean hasPlanner() {
    return config.hasPath(PLANNER_TYPE);
  }

  public boolean hasPartitioner() {
    return config.hasPath(PARTITIONER_TYPE);
  }

  public boolean hasOutput() {
    return config.hasPath(OUTPUT_TYPE);
  }
  
  protected Input getInput(boolean configure) {
    Config inputConfig = config.getConfig(INPUT_TYPE);

    if (configure) {
      if (input == null) {
        input = InputFactory.create(inputConfig, configure);
      }
      return input;
    }
    else {
      return InputFactory.create(inputConfig, configure);
    }
  }
  
  protected Deriver getDeriver(boolean configure) {
    Config deriverConfig = config.getConfig(DERIVER_TYPE);

    if (configure) {
      if (deriver == null) {
        deriver = DeriverFactory.create(deriverConfig, configure);
      }
      return deriver;
    }
    else {
      return DeriverFactory.create(deriverConfig, configure);
    }
  }

  protected Planner getPlanner(boolean configure) {
    Config plannerConfig = config.getConfig(PLANNER_TYPE);

    if (configure) {
      if (planner == null) {
        planner = PlannerFactory.create(plannerConfig, configure);
      }
      return planner;
    }
    else {
      return PlannerFactory.create(plannerConfig, configure);
    }
  }

  protected Output getOutput(boolean configure) {
    Config outputConfig = config.getConfig(OUTPUT_TYPE);

    if (configure) {
      if (output == null) {
        output = OutputFactory.create(outputConfig, configure);
      }
      return output;
    }
    else {
      return OutputFactory.create(outputConfig, configure);
    }
  }

  private void registerStep() {
    data.createOrReplaceTempView(getName());
  }

  private boolean doesCache() {
    return ConfigUtils.getOrElse(config, CACHE_ENABLED_PROPERTY, false);
  }
  
  private void cache() {
    String cacheLevel = "MEMORY_ONLY";
    if (config.hasPath(CACHE_STORAGE_LEVEL_PROPERTY)) { 
      cacheLevel = config.getString(CACHE_STORAGE_LEVEL_PROPERTY);
    }
    
    switch(cacheLevel){
      case "DISK_ONLY":
        data.persist(StorageLevel.DISK_ONLY());
        break;
      case "DISK_ONLY_2":
        data.persist(StorageLevel.DISK_ONLY_2());
        break;
      case "MEMORY_ONLY":
        data.persist(StorageLevel.MEMORY_ONLY());
        break;
      case "MEMORY_ONLY_2":
        data.persist(StorageLevel.MEMORY_ONLY_2());
        break;
      case "MEMORY_ONLY_SER":
        data.persist(StorageLevel.MEMORY_ONLY_SER());
        break;
      case "MEMORY_ONLY_SER_2":
        data.persist(StorageLevel.MEMORY_ONLY_SER_2());
        break;
      case "MEMORY_AND_DISK":
        data.persist(StorageLevel.MEMORY_AND_DISK());
        break;
      case "MEMORY_AND_DISK_2":
        data.persist(StorageLevel.MEMORY_AND_DISK_2());
        break;
      case "MEMORY_AND_DISK_SER":
        data.persist(StorageLevel.MEMORY_AND_DISK_SER());
        break;
      case "MEMORY_AND_DISK_SER_2":
        data.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
        break;
      case "OFF_HEAP":
        data.persist(StorageLevel.OFF_HEAP());
        break;
      default:
        throw new RuntimeException("Invalid value for cache.storage.level property");
    }
  }

  public void clearCache() {
    data = data.unpersist(false);
  }

  private boolean usesSmallHint() {
    return ConfigUtils.getOrElse(config, SMALL_HINT_PROPERTY, false);
  }

  private void applySmallHint() {
    data = functions.broadcast(data);
  }
  
  private boolean doesPrintSchema() {
    return ConfigUtils.getOrElse(config, PRINT_SCHEMA_ENABLED_PROPERTY, false);
  }
  
  private void printSchema() {
    System.out.println("Schema for step " + getName() + ":");
    
    data.printSchema();
  }
  
  private boolean doesPrintData() {
    return ConfigUtils.getOrElse(config, PRINT_DATA_ENABLED_PROPERTY, false);
  }
  
  private void printData() {
    if (config.hasPath(PRINT_DATA_LIMIT_PROPERTY)) {
      int limit = config.getInt(PRINT_DATA_LIMIT_PROPERTY);
      data.limit(limit).show();
    }
    else {
      data.show();
    }
  }
  
  @Override
  public Set<AccumulatorRequest> getAccumulatorRequests() {
    Set<AccumulatorRequest> requests = Sets.newHashSet();
    
    if (hasInput() && getInput(false) instanceof UsesAccumulators) {
      requests.addAll(((UsesAccumulators)getInput(false)).getAccumulatorRequests());
    }
    if (hasDeriver() && getDeriver(false) instanceof UsesAccumulators) {
      requests.addAll(((UsesAccumulators)getDeriver(false)).getAccumulatorRequests());
    }
    if (hasPlanner() && getPlanner(false) instanceof UsesAccumulators) {
      requests.addAll(((UsesAccumulators)getPlanner(false)).getAccumulatorRequests());
    }
    if (hasOutput() && getOutput(false) instanceof UsesAccumulators) {
      requests.addAll(((UsesAccumulators)getOutput(false)).getAccumulatorRequests());
    }
    
    requests.add(new AccumulatorRequest(ACCUMULATOR_SECONDS_PLANNING, Double.class));
    requests.add(new AccumulatorRequest(ACCUMULATOR_SECONDS_APPLYING, Double.class));
    requests.add(new AccumulatorRequest(ACCUMULATOR_SECONDS_EXISTING, Double.class));
    requests.add(new AccumulatorRequest(ACCUMULATOR_SECONDS_EXTRACTING_KEYS, Double.class));
    
    return requests;
  }
  
  @Override
  public void receiveAccumulators(Accumulators accumulators) {
    this.accumulators = accumulators;
    
    if (hasInput() && getInput(false) instanceof UsesAccumulators) {
      ((UsesAccumulators)getInput(false)).receiveAccumulators(accumulators);
    }
    if (hasDeriver() && getDeriver(false) instanceof UsesAccumulators) {
      ((UsesAccumulators)getDeriver(false)).receiveAccumulators(accumulators);
    }
    if (hasPlanner() && getPlanner(false) instanceof UsesAccumulators) {
      ((UsesAccumulators)getPlanner(false)).receiveAccumulators(accumulators);
    }
    if (hasOutput() && getOutput(false) instanceof UsesAccumulators) {
      ((UsesAccumulators)getOutput(false)).receiveAccumulators(accumulators);
    }
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean configure) {
    this.config = config;

    Set<InstantiatedComponent> validatables = Sets.newHashSet();

    if (hasInput()) {
      validatables.add(new InstantiatedComponent(
          getInput(configure), config.getConfig(INPUT_TYPE), "Input"));
    }
    if (hasDeriver()) {
      validatables.add(new InstantiatedComponent(
          getDeriver(configure), config.getConfig(DERIVER_TYPE), "Deriver"));
    }
    if (hasPlanner()) {
      validatables.add(new InstantiatedComponent(
          getPlanner(configure), config.getConfig(PLANNER_TYPE), "Planner"));
    }
    if (hasOutput()) {
      validatables.add(new InstantiatedComponent(
          getOutput(configure), config.getConfig(OUTPUT_TYPE), "Output"));
    }

    return validatables;
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .optionalPath(CACHE_ENABLED_PROPERTY, ConfigValueType.BOOLEAN)
        .optionalPath(CACHE_STORAGE_LEVEL_PROPERTY, ConfigValueType.STRING)
        .optionalPath(SMALL_HINT_PROPERTY, ConfigValueType.BOOLEAN)
        .optionalPath(PRINT_SCHEMA_ENABLED_PROPERTY, ConfigValueType.BOOLEAN)
        .optionalPath(PRINT_DATA_ENABLED_PROPERTY, ConfigValueType.BOOLEAN)
        .ifPathHasValue(PRINT_DATA_ENABLED_PROPERTY, true,
            Validations.single().optionalPath(PRINT_DATA_LIMIT_PROPERTY, ConfigValueType.NUMBER))
        .optionalPath(INPUT_TYPE, ConfigValueType.OBJECT)
        .optionalPath(DERIVER_TYPE, ConfigValueType.OBJECT)
        .optionalPath(PLANNER_TYPE, ConfigValueType.OBJECT)
        .optionalPath(PARTITIONER_TYPE, ConfigValueType.OBJECT)
        .optionalPath(OUTPUT_TYPE, ConfigValueType.OBJECT)
        .exactlyOnePathExists(INPUT_TYPE, DERIVER_TYPE)
        .handlesOwnValidationPath(INPUT_TYPE)
        .handlesOwnValidationPath(DERIVER_TYPE)
        .handlesOwnValidationPath(PLANNER_TYPE)
        .handlesOwnValidationPath(PARTITIONER_TYPE)
        .handlesOwnValidationPath(OUTPUT_TYPE)
        .addAll(super.getValidations())
        .build();
  }

  private void writeOutput() {
    Config plannerConfig = config.getConfig(PLANNER_TYPE);
    Planner planner = getPlanner(true);
    validatePlannerOutputCompatibility(planner, getOutput(false));

    // Plan the mutations, and then apply them to the output, based on the type of planner used
    if (planner instanceof RandomPlanner) {
      RandomPlanner randomPlanner = (RandomPlanner)planner;
      List<String> keyFieldNames = randomPlanner.getKeyFieldNames();
      Config outputConfig = config.getConfig(OUTPUT_TYPE);
      JavaRDD<Row> planned = planMutationsByKey(data, keyFieldNames, plannerConfig, outputConfig);

      applyMutations(planned, outputConfig);
    }
    else if (planner instanceof BulkPlanner) {
      BulkPlanner bulkPlanner = (BulkPlanner)planner;
      List<Tuple2<MutationType, Dataset<Row>>> planned = bulkPlanner.planMutationsForSet(data);

      BulkOutput bulkOutput = (BulkOutput)getOutput(true);
      bulkOutput.applyBulkMutations(planned);
    }
    else {
      throw new RuntimeException("Unexpected planner class: " + planner.getClass().getName());
    }
  }

  private void validatePlannerOutputCompatibility(Planner planner, Output output) {
    Set<MutationType> plannerMTs = planner.getEmittedMutationTypes();

    if (planner instanceof RandomPlanner) {
      if (!(output instanceof RandomOutput)) {
        handleIncompatiblePlannerOutput(planner, output);
      }

      Set<MutationType> outputMTs = ((RandomOutput)output).getSupportedRandomMutationTypes();

      for (MutationType planMT : plannerMTs) {
        if (!outputMTs.contains(planMT)) {
          handleIncompatiblePlannerOutput(planner, output);
        }
      }
    }
    else if (planner instanceof BulkPlanner) {
      if (!(output instanceof BulkOutput)) {
        handleIncompatiblePlannerOutput(planner, output);
      }

      Set<MutationType> outputMTs = ((BulkOutput)output).getSupportedBulkMutationTypes();

      for (MutationType planMT : plannerMTs) {
        if (!outputMTs.contains(planMT)) {
          handleIncompatiblePlannerOutput(planner, output);
        }
      }
    }
    else {
      throw new RuntimeException("Unexpected planner class: " + planner.getClass().getName());
    }
  }

  private void handleIncompatiblePlannerOutput(Planner planner, Output output) {
    throw new RuntimeException("Incompatible planner (" + planner.getClass() +
        ") and output (" + output.getClass() + ").");
  }
  
  // Group the arriving records by key, attach the existing records for each key, and plan
  private JavaRDD<Row> planMutationsByKey(Dataset<Row> arriving, List<String> keyFieldNames,
                                          Config plannerConfig, Config outputConfig) {
    JavaPairRDD<Row, Row> keyedArriving = 
        arriving.javaRDD().keyBy(new ExtractKeyFunction(keyFieldNames, accumulators));

    JavaPairRDD<Row, Iterable<Row>> arrivingByKey = 
        keyedArriving.groupByKey(getPartitioner(keyedArriving));

    JavaPairRDD<Row, Tuple2<Iterable<Row>, Iterable<Row>>> arrivingAndExistingByKey =
        arrivingByKey.mapPartitionsToPair(new JoinExistingForKeysFunction(outputConfig, keyFieldNames, accumulators));

    JavaRDD<Row> planned = 
        arrivingAndExistingByKey.flatMap(new PlanForKeyFunction(plannerConfig, accumulators));

    return planned;
  }

  @SuppressWarnings("serial")
  private static class ExtractKeyFunction implements Function<Row, Row> {
    private StructType schema;
    private List<String> keyFieldNames;
    private Accumulators accumulators;

    public ExtractKeyFunction(List<String> keyFieldNames, Accumulators accumulators) {
      this.keyFieldNames = keyFieldNames;
      this.accumulators = accumulators;
    }

    @Override
    public Row call(Row arrived) throws Exception {
      long startTime = System.nanoTime();

      if (schema == null) {
        schema = RowUtils.subsetSchema(arrived.schema(), keyFieldNames);
      }

      Row key = RowUtils.subsetRow(arrived, schema);
      
      long endTime = System.nanoTime();
      accumulators.getDoubleAccumulators().get(ACCUMULATOR_SECONDS_EXTRACTING_KEYS).add(
          (endTime - startTime) / 1000.0 / 1000.0 / 1000.0);

      return key;
    }
  }
  
  private Partitioner getPartitioner(JavaPairRDD<Row, Row> keyedArriving) {
    Config partitionerConfig;
    
    if (hasPartitioner()) {
      partitionerConfig = config.getConfig(PARTITIONER_TYPE);
    }
    else {
      partitionerConfig = ConfigFactory.empty().withValue(
          PartitionerFactory.TYPE_CONFIG_NAME, ConfigValueFactory.fromAnyRef("range"));
    }
    
    return PartitionerFactory.create(partitionerConfig, keyedArriving);
  }
  
  @SuppressWarnings("serial")
  private static class JoinExistingForKeysFunction
  implements PairFlatMapFunction<Iterator<Tuple2<Row, Iterable<Row>>>, Row, Tuple2<Iterable<Row>, Iterable<Row>>> {
    private Config outputConfig;
    private RandomOutput output;
    private List<String> keyFieldNames;
    private Accumulators accumulators;

    public JoinExistingForKeysFunction(Config outputConfig, List<String> keyFieldNames, Accumulators accumulators) {
      this.outputConfig = outputConfig;
      this.keyFieldNames = keyFieldNames;
      this.accumulators = accumulators;
    }

    // Add the existing records for the keys to the arriving records
    @Override
    public Iterator<Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>>>
    call(Iterator<Tuple2<Row, Iterable<Row>>> arrivingForKeysIterator) throws Exception
    {
      // If there are no arriving keys, return an empty list
      if (!arrivingForKeysIterator.hasNext()) {
        return Lists.<Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>>>newArrayList().iterator();
      }
      
      long startTime = System.nanoTime();

      // If we have not instantiated the output for this partition, instantiate it
      if (output == null) {
        output = (RandomOutput)OutputFactory.create(outputConfig, true);
        if (output instanceof UsesAccumulators) {
            ((UsesAccumulators)output).receiveAccumulators(accumulators);
        }
      }

      // Convert the iterator of keys to a list
      List<Tuple2<Row, Iterable<Row>>> arrivingForKeys = Lists.newArrayList(arrivingForKeysIterator);

      // Extract the keys from the keyed arriving records
      Set<Row> arrivingKeys = extractKeys(arrivingForKeys);

      // Get the existing records for those keys from the output
      Iterable<Row> existingWithoutKeys = output.getExistingForFilters(arrivingKeys);
      
      // Map the retrieved existing records to the keys they were looked up from
      Map<Row, Iterable<Row>> existingForKeys = mapExistingToKeys(existingWithoutKeys);

      // Attach the existing records by key to the arriving records by key
      List<Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>>> arrivingAndExistingForKeys = 
          attachExistingToArrivingForKeys(existingForKeys, arrivingForKeys);
      
      long endTime = System.nanoTime();
      accumulators.getDoubleAccumulators().get(ACCUMULATOR_SECONDS_EXISTING).add(
          (endTime - startTime) / 1000.0 / 1000.0 / 1000.0);

      return arrivingAndExistingForKeys.iterator();
    }

    private Set<Row> extractKeys(List<Tuple2<Row, Iterable<Row>>> arrivingForKeys) {
      Set<Row> arrivingKeys = Sets.newHashSet();

      for (Tuple2<Row, Iterable<Row>> arrivingForKey : arrivingForKeys) {
        arrivingKeys.add(arrivingForKey._1());
      }

      return arrivingKeys;
    }

    private Map<Row, Iterable<Row>> mapExistingToKeys(Iterable<Row> existingWithoutKeys) throws Exception {
      Map<Row, Iterable<Row>> existingForKeys = Maps.newHashMap();
      ExtractKeyFunction extractKeyFunction = new ExtractKeyFunction(keyFieldNames, accumulators);

      for (Row existing : existingWithoutKeys) {
        Row existingKey = extractKeyFunction.call(existing);

        if (!existingForKeys.containsKey(existingKey)) {
          existingForKeys.put(existingKey, Lists.<Row>newArrayList());
        }

        ((List<Row>)existingForKeys.get(existingKey)).add(existing);
      }

      return existingForKeys;
    }

    private List<Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>>> attachExistingToArrivingForKeys
    (Map<Row, Iterable<Row>> existingForKeys, List<Tuple2<Row, Iterable<Row>>> arrivingForKeys)
    {
      List<Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>>> arrivingAndExistingForKeys = Lists.newArrayList();
      for (Tuple2<Row, Iterable<Row>> arrivingForKey : arrivingForKeys) {
        Row key = arrivingForKey._1();
        Iterable<Row> arriving = arrivingForKey._2();

        Iterable<Row> existing;
        if (existingForKeys.containsKey(key)) {
          existing = existingForKeys.get(key);
        }
        else {
          existing = Lists.newArrayList();
        }

        // Oh my...
        Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>> arrivingAndExistingForKey = 
            new Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>>(key, 
                new Tuple2<Iterable<Row>, Iterable<Row>>(arriving, existing));

        arrivingAndExistingForKeys.add(arrivingAndExistingForKey);
      }

      return arrivingAndExistingForKeys;
    }
  }

  @SuppressWarnings("serial")
  private static class PlanForKeyFunction
  implements FlatMapFunction<Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>>, Row> {
    private Config config;
    private RandomPlanner planner;
    private Accumulators accumulators;

    public PlanForKeyFunction(Config config, Accumulators accumulators) {
      this.config = config;
      this.accumulators = accumulators;
    }

    @Override
    public Iterator<Row>
    call(Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>> keyedRecords) throws Exception {
      long startTime = System.nanoTime();
      
      if (planner == null) {
        planner = (RandomPlanner)PlannerFactory.create(config, true);
        if (planner instanceof UsesAccumulators) {
          ((UsesAccumulators)planner).receiveAccumulators(accumulators);
        }
      }

      Row key = keyedRecords._1();
      List<Row> arrivingRecords = Lists.newArrayList(keyedRecords._2()._1());
      List<Row> existingRecords = Lists.newArrayList(keyedRecords._2()._2());

      Iterable<Row> plannedForKey = planner.planMutationsForKey(key, arrivingRecords, existingRecords);
      
      long endTime = System.nanoTime();
      accumulators.getDoubleAccumulators().get(ACCUMULATOR_SECONDS_PLANNING).add(
          (endTime - startTime) / 1000.0 / 1000.0 / 1000.0);

      return plannedForKey.iterator();
    }
  };

  private void applyMutations(JavaRDD<Row> planned, Config outputConfig) {
    planned.foreachPartition(new ApplyMutationsForPartitionFunction(outputConfig, accumulators));
  }

  @SuppressWarnings("serial")
  private static class ApplyMutationsForPartitionFunction implements VoidFunction<Iterator<Row>> {
    private Config config;
    private RandomOutput output;
    private Accumulators accumulators;

    public ApplyMutationsForPartitionFunction(Config config, Accumulators accumulators) {
      this.config = config;
      this.accumulators = accumulators;
    }

    @Override
    public void call(Iterator<Row> plannedIterator) throws Exception {
      long startTime = System.nanoTime();

      if (output == null) {
        output = (RandomOutput)OutputFactory.create(config, true);
        if (output instanceof UsesAccumulators) {
          ((UsesAccumulators)output).receiveAccumulators(accumulators);
        }
      }
      
      List<Row> planned = Lists.newArrayList(plannedIterator);

      output.applyRandomMutations(planned);
      
      long endTime = System.nanoTime();
      accumulators.getDoubleAccumulators().get(ACCUMULATOR_SECONDS_APPLYING).add(
          (endTime - startTime) / 1000.0 / 1000.0 / 1000.0);
    }
  }

}
