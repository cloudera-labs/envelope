/**
 * Copyright © 2016-2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.run;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.HashPartitioner;
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
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.cloudera.labs.envelope.plan.Planner;
import com.cloudera.labs.envelope.plan.PlannerFactory;
import com.cloudera.labs.envelope.plan.RandomPlanner;
import com.cloudera.labs.envelope.spark.AccumulatorRequest;
import com.cloudera.labs.envelope.spark.Accumulators;
import com.cloudera.labs.envelope.spark.UsesAccumulators;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * A data step is a step that will contain a DataFrame that other steps can use.
 * The DataFrame can be created either by an input or a deriver.
 * The DataFrame can be optionally written to an output, as planned by the planner.
 */
public abstract class DataStep extends Step implements UsesAccumulators {

  public static final String CACHE_PROPERTY = "cache";
  public static final String SMALL_HINT_PROPERTY = "hint.small";
  public static final String PRINT_SCHEMA_ENABLED_PROPERTY = "print.schema.enabled";
  public static final String PRINT_DATA_ENABLED_PROPERTY = "print.data.enabled";
  public static final String PRINT_DATA_LIMIT_PROPERTY = "print.data.limit";
  
  private static final String ACCUMULATOR_SECONDS_EXTRACTING_KEYS = "Seconds spent extracting keys";
  private static final String ACCUMULATOR_SECONDS_EXISTING = "Seconds spent getting existing";
  private static final String ACCUMULATOR_SECONDS_PLANNING = "Seconds spent random planning";
  private static final String ACCUMULATOR_SECONDS_APPLYING = "Seconds spent applying random mutations";

  protected boolean finished = false;
  protected Dataset<Row> data;
  protected Input input;
  protected Deriver deriver;
  protected Planner planner;
  protected Output output;
  protected Accumulators accumulators;

  public DataStep(String name, Config config) throws Exception {
    super(name, config);

    if (hasInput() && hasDeriver()) {
      throw new RuntimeException("Steps can not have both an input and a deriver");
    }

    if (hasInput()) {
      Config inputConfig = config.getConfig("input");
      input = InputFactory.create(inputConfig);
    }
    if (hasDeriver()) {
      Config deriverConfig = config.getConfig("deriver");
      deriver = DeriverFactory.create(deriverConfig);
    }
    if (hasPlanner()) {
      Config plannerConfig = config.getConfig("planner");
      planner = PlannerFactory.create(plannerConfig);
    }
    if (hasOutput()) {
      Config outputConfig = config.getConfig("output");
      output = OutputFactory.create(outputConfig);
    }
  }

  public boolean hasFinished() {
    return finished;
  }

  public void setFinished(boolean finished) {
    this.finished = finished;
  }

  public Dataset<Row> getData() {
    return data;  
  }

  public void setData(Dataset<Row> batchDF) throws Exception {
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
  
  private void registerStep() {
    data.createOrReplaceTempView(getName());
  }

  private boolean doesCache() {
    if (!config.hasPath(CACHE_PROPERTY)) return true;

    return config.getBoolean(CACHE_PROPERTY);
  }

  private void cache() {
    data = data.persist(StorageLevel.MEMORY_ONLY());
  }

  public void clearCache() {
    data = data.unpersist(false);
  }

  private boolean usesSmallHint() {
    if (!config.hasPath(SMALL_HINT_PROPERTY)) return false;

    return config.getBoolean(SMALL_HINT_PROPERTY);
  }

  private void applySmallHint() {
    data = functions.broadcast(data);
  }
  
  private boolean doesPrintSchema() {
    if (!config.hasPath(PRINT_SCHEMA_ENABLED_PROPERTY)) return false;

    return config.getBoolean(PRINT_SCHEMA_ENABLED_PROPERTY);
  }
  
  private void printSchema() {
    System.out.println("Schema for step " + getName() + ":");
    
    data.printSchema();
  }
  
  private boolean doesPrintData() {
    if (!config.hasPath(PRINT_DATA_ENABLED_PROPERTY)) return false;

    return config.getBoolean(PRINT_DATA_ENABLED_PROPERTY);
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

  protected Map<String, Dataset<Row>> getStepDataFrames(Set<Step> steps) {
    Map<String, Dataset<Row>> stepDFs = Maps.newHashMap();

    for (Step step : steps) {
      if (step instanceof DataStep) {
        stepDFs.put(step.getName(), ((DataStep)step).getData());
      }
    }

    return stepDFs;
  }
  
  @Override
  public Set<AccumulatorRequest> getAccumulatorRequests() {
    Set<AccumulatorRequest> requests = Sets.newHashSet();
    
    if (hasInput() && input instanceof UsesAccumulators) {
      requests.addAll(((UsesAccumulators)input).getAccumulatorRequests());
    }
    if (hasDeriver() && deriver instanceof UsesAccumulators) {
      requests.addAll(((UsesAccumulators)deriver).getAccumulatorRequests());
    }
    if (hasPlanner() && planner instanceof UsesAccumulators) {
      requests.addAll(((UsesAccumulators)planner).getAccumulatorRequests());
    }
    if (hasOutput() && output instanceof UsesAccumulators) {
      requests.addAll(((UsesAccumulators)output).getAccumulatorRequests());
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
    
    if (hasInput() && input instanceof UsesAccumulators) {
      ((UsesAccumulators)input).receiveAccumulators(accumulators);
    }
    if (hasDeriver() && deriver instanceof UsesAccumulators) {
      ((UsesAccumulators)deriver).receiveAccumulators(accumulators);
    }
    if (hasPlanner() && planner instanceof UsesAccumulators) {
      ((UsesAccumulators)planner).receiveAccumulators(accumulators);
    }
    if (hasOutput() && output instanceof UsesAccumulators) {
      ((UsesAccumulators)output).receiveAccumulators(accumulators);
    }
  }

  public boolean hasInput() {
    return config.hasPath("input");
  }

  public boolean hasDeriver() {
    return config.hasPath("deriver");
  }
  
  public boolean hasPlanner() {
    return config.hasPath("planner");
  }
    
  public boolean hasPartitioner() {
    return config.hasPath("partitioner");
  }

  public boolean hasOutput() {
    return config.hasPath("output");
  }

  private void writeOutput() throws Exception {
    Config plannerConfig = config.getConfig("planner");
    validatePlannerOutputCompatibility(planner, output);

    // Plan the mutations, and then apply them to the output, based on the type of planner used
    if (planner instanceof RandomPlanner) {      
      RandomPlanner randomPlanner = (RandomPlanner)planner;
      List<String> keyFieldNames = randomPlanner.getKeyFieldNames();
      Config outputConfig = config.getConfig("output");
      JavaRDD<PlannedRow> planned = planMutationsByKey(data, keyFieldNames, plannerConfig, outputConfig);

      applyMutations(planned, outputConfig);
    }
    else if (planner instanceof BulkPlanner) {
      BulkPlanner bulkPlanner = (BulkPlanner)planner;
      List<Tuple2<MutationType, Dataset<Row>>> planned = bulkPlanner.planMutationsForSet(data);

      BulkOutput bulkOutput = (BulkOutput)output;      
      bulkOutput.applyBulkMutations(planned);
    }
    else {
      throw new RuntimeException("Unexpected output class: " + output.getClass().getName());
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
    throw new RuntimeException("Incompatible planner (" + planner.getClass() + ") and output (" + output.getClass() + ").");
  }
  
  // Group the arriving records by key, attach the existing records for each key, and plan
  private JavaRDD<PlannedRow> planMutationsByKey(Dataset<Row> arriving, List<String> keyFieldNames, Config plannerConfig, Config outputConfig) throws Exception  {
    JavaPairRDD<Row, Row> keyedArriving = 
        arriving.javaRDD().keyBy(new ExtractKeyFunction(keyFieldNames, accumulators));

    JavaPairRDD<Row, Iterable<Row>> arrivingByKey = 
        keyedArriving.groupByKey(getPartitioner(keyedArriving));

    JavaPairRDD<Row, Tuple2<Iterable<Row>, Iterable<Row>>> arrivingAndExistingByKey =
        arrivingByKey.mapPartitionsToPair(new JoinExistingForKeysFunction(outputConfig, keyFieldNames, accumulators));

    JavaRDD<PlannedRow> planned = 
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
      accumulators.getDoubleAccumulators().get(ACCUMULATOR_SECONDS_EXTRACTING_KEYS).add((endTime - startTime) / 1000.0 / 1000.0 / 1000.0);

      return key;
    }
  }
  
  private Partitioner getPartitioner(JavaPairRDD<Row, Row> keyedArriving) throws Exception {    
    if (hasPartitioner()) {
      Config partitionerConfig = config.getConfig("partitioner");      
      return PartitionerFactory.create(partitionerConfig, keyedArriving); 
    }
    else {
      return new HashPartitioner(keyedArriving.getNumPartitions());
    }
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
        output = (RandomOutput)OutputFactory.create(outputConfig);
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
      accumulators.getDoubleAccumulators().get(ACCUMULATOR_SECONDS_EXISTING).add((endTime - startTime) / 1000.0 / 1000.0 / 1000.0);

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
  implements FlatMapFunction<Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>>, PlannedRow> {
    private Config config;
    private RandomPlanner planner;
    private Accumulators accumulators;

    public PlanForKeyFunction(Config config, Accumulators accumulators) {
      this.config = config;
      this.accumulators = accumulators;
    }

    @Override
    public Iterator<PlannedRow>
    call(Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>> keyedRecords) throws Exception {
      long startTime = System.nanoTime();
      
      if (planner == null) {
        planner = (RandomPlanner)PlannerFactory.create(config);
        if (planner instanceof UsesAccumulators) {
          ((UsesAccumulators)planner).receiveAccumulators(accumulators);
        }
      }

      Row key = keyedRecords._1();
      List<Row> arrivingRecords = Lists.newArrayList(keyedRecords._2()._1());
      List<Row> existingRecords = Lists.newArrayList(keyedRecords._2()._2());

      Iterable<PlannedRow> plannedForKey = planner.planMutationsForKey(key, arrivingRecords, existingRecords);
      
      long endTime = System.nanoTime();
      accumulators.getDoubleAccumulators().get(ACCUMULATOR_SECONDS_PLANNING).add((endTime - startTime) / 1000.0 / 1000.0 / 1000.0);

      return plannedForKey.iterator();
    }
  };

  private void applyMutations(JavaRDD<PlannedRow> planned, Config outputConfig) {
    planned.foreachPartition(new ApplyMutationsForPartitionFunction(outputConfig, accumulators));
  }

  @SuppressWarnings("serial")
  private static class ApplyMutationsForPartitionFunction implements VoidFunction<Iterator<PlannedRow>> {
    private Config config;
    private RandomOutput output;
    private Accumulators accumulators;

    public ApplyMutationsForPartitionFunction(Config config, Accumulators accumulators) {
      this.config = config;
      this.accumulators = accumulators;
    }

    @Override
    public void call(Iterator<PlannedRow> plannedIterator) throws Exception {
      long startTime = System.nanoTime();

      if (output == null) {
        output = (RandomOutput)OutputFactory.create(config);
        if (output instanceof UsesAccumulators) {
          ((UsesAccumulators)output).receiveAccumulators(accumulators);
        }
      }
      
      List<PlannedRow> planned = Lists.newArrayList(plannedIterator);

      output.applyRandomMutations(planned);
      
      long endTime = System.nanoTime();
      accumulators.getDoubleAccumulators().get(ACCUMULATOR_SECONDS_APPLYING).add((endTime - startTime) / 1000.0 / 1000.0 / 1000.0);
    }
  }

}
