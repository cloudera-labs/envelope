package com.cloudera.labs.envelope.run;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kudu.client.shaded.com.google.common.collect.Sets;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import com.cloudera.labs.envelope.derive.Deriver;
import com.cloudera.labs.envelope.input.Input;
import com.cloudera.labs.envelope.output.Output;
import com.cloudera.labs.envelope.output.bulk.BulkWriteOutput;
import com.cloudera.labs.envelope.output.random.RandomReadWriteOutput;
import com.cloudera.labs.envelope.output.random.RandomWriteOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRow;
import com.cloudera.labs.envelope.plan.Planner;
import com.cloudera.labs.envelope.plan.bulk.BulkWritePlanner;
import com.cloudera.labs.envelope.plan.random.RandomReadWritePlanner;
import com.cloudera.labs.envelope.plan.random.RandomWritePlanner;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import scala.Tuple2;

public abstract class DataStep extends Step {
    
    public static final String CACHE_PROPERTY = "cache";
    public static final String SMALL_HINT_PROPERTY = "hint.small";
    
    protected boolean finished = false;
    protected DataFrame data;
    protected Input input;
    protected Deriver deriver;
    protected Output output;
    
    public DataStep(String name, Config config) throws Exception {
        super(name, config);
        
        if (hasInput() && hasDeriver()) {
            throw new RuntimeException("Steps can not have both an input and a deriver");
        }
        
        if (hasInput()) {
            Config inputConfig = config.getConfig("input");
            input = Input.inputFor(inputConfig);
        }
        if (hasDeriver()) {
            Config deriverConfig = config.getConfig("deriver");
            deriver = Deriver.deriverFor(deriverConfig);
        }
        if (hasOutput()) {
            Config outputConfig = config.getConfig("output");
            output = Output.outputFor(outputConfig);
        }
    }
    
    public boolean hasFinished() {
        return finished;
    }
    
    public void setFinished(boolean finished) {
        this.finished = finished;
    }
    
    public DataFrame getData() {
        return data;  
    }
    
    public void setData(DataFrame data) throws Exception {
        this.data = data;
        
        data.registerTempTable(getName());
        
        if (doesCache()) {
            cache();
        }
        
        if (usesSmallHint()) {
            applySmallHint();
        }
        
        if (hasOutput()) {
            writeOutput();
        }
    }
    
    private boolean doesCache() {
        if (!config.hasPath(CACHE_PROPERTY)) return true;
        
        return config.getBoolean(CACHE_PROPERTY);
    }
    
    private void cache() {
        data.persist(StorageLevel.MEMORY_ONLY());
    }
    
    public void clearCache() {
        data.unpersist(false);
    }
    
    private boolean usesSmallHint() {
        if (!config.hasPath(SMALL_HINT_PROPERTY)) return false;
        
        return config.getBoolean(SMALL_HINT_PROPERTY);
    }
    
    private void applySmallHint() {
        data = functions.broadcast(data);
    }
    
    protected Map<String, DataFrame> getStepDataFrames(Set<Step> steps) {
        Map<String, DataFrame> stepDFs = Maps.newHashMap();
        
        for (Step step : steps) {
            if (step instanceof DataStep) {
                stepDFs.put(step.getName(), ((DataStep)step).getData());
            }
        }
        
        return stepDFs;
    }
    
    public boolean hasInput() {
        return config.hasPath("input");
    }
    
    public boolean hasDeriver() {
        return config.hasPath("deriver");
    }
    
    public boolean hasOutput() {
        return config.hasPath("output");
    }
    
    private void writeOutput() throws Exception {
        Config outputConfig = config.getConfig("output");
        Config plannerConfig = config.getConfig("planner");
        Planner planner = Planner.plannerFor(plannerConfig);
        validatePlannerStorageCompatibility(planner, output);
        
        if (output instanceof RandomWriteOutput) {            
            if (planner instanceof RandomWritePlanner) {
                JavaRDD<PlannedRow> planned = planMutationsByRow(data, plannerConfig);
                
                applyMutations(planned, outputConfig);
            }
            else if (planner instanceof RandomReadWritePlanner) {
                RandomReadWritePlanner randomReadWritePlanner = (RandomReadWritePlanner)planner;
                List<String> keyFieldNames = randomReadWritePlanner.getKeyFieldNames();
                JavaRDD<PlannedRow> planned = planMutationsByKey(data, keyFieldNames, plannerConfig, outputConfig);
                
                applyMutations(planned, outputConfig);
            }
            else {
                throw new RuntimeException("Unexpected planner class: " + planner.getClass().getName());
            }
        }
        else if (output instanceof BulkWriteOutput) {
            BulkWritePlanner bulkPlanner = (BulkWritePlanner)planner;
            List<Tuple2<MutationType, DataFrame>> planned = bulkPlanner.planMutationsForSet(data);
            
            BulkWriteOutput bulkOutput = (BulkWriteOutput)output;            
            bulkOutput.applyMutations(planned);
        }
        else {
            throw new RuntimeException("Unexpected output class: " + output.getClass().getName());
        }
    }
    
    private void validatePlannerStorageCompatibility(Planner planner, Output output) {
        if ((planner instanceof RandomReadWritePlanner) && !(output instanceof RandomReadWriteOutput)) {
            throw new RuntimeException("Incompatible planner (" + planner.getClass() + ") and output (" + output.getClass() + ").");
        }
        
        Set<MutationType> outputMTs = output.getSupportedMutationTypes();
        Set<MutationType> plannerMTs = planner.getEmittedMutationTypes();
        
        for (MutationType planMT : plannerMTs) {
            if (!outputMTs.contains(planMT)) {
                throw new RuntimeException("Incompatible planner (" + planner.getClass() + ") and output (" + output.getClass() + ").");
            }
        }
    }
    
    private JavaRDD<PlannedRow> planMutationsByRow(DataFrame arriving, Config plannerConfig) {
        JavaRDD<PlannedRow> planned = arriving.javaRDD().flatMap(new PlanForRowFunction(plannerConfig));
        
        return planned;
    }
    
    @SuppressWarnings("serial")
    private static class PlanForRowFunction implements FlatMapFunction<Row, PlannedRow> {
        private Config config;
        private RandomWritePlanner planner;
        
        public PlanForRowFunction(Config config) {
            this.config = config;
        }
        
        @Override
        public Iterable<PlannedRow> call(Row arrived) throws Exception {
            if (planner == null) {
                planner = (RandomWritePlanner)Planner.plannerFor(config);
            }
            
            Iterable<PlannedRow> plan = planner.planMutationsForRow(arrived);
            
            return plan;
        }
    }
    
    private JavaRDD<PlannedRow> planMutationsByKey(DataFrame arriving, List<String> keyFieldNames, Config plannerConfig, Config outputConfig) {
        JavaPairRDD<Row, Iterable<Row>> arrivingByKey = 
                arriving.javaRDD().groupBy(new ExtractKeyFunction(keyFieldNames));
        
        JavaPairRDD<Row, Tuple2<Iterable<Row>, Iterable<Row>>> arrivingAndExistingByKey =
                arrivingByKey.mapPartitionsToPair(new JoinExistingForKeysFunction(outputConfig, keyFieldNames));
        
        JavaRDD<PlannedRow> planned = 
                arrivingAndExistingByKey.flatMap(new PlanForKeyFunction(plannerConfig));
        
        return planned;
    }
    
    @SuppressWarnings("serial")
    private static class ExtractKeyFunction implements Function<Row, Row> {
        private StructType schema;
        private List<String> keyFieldNames;
        
        public ExtractKeyFunction(List<String> keyFieldNames) {
            this.keyFieldNames = keyFieldNames;
        }
        
        @Override
        public Row call(Row arrived) throws Exception {
            if (schema == null) {
                schema = RowUtils.subsetSchema(arrived.schema(), keyFieldNames);
            }
            
            Row key = RowUtils.subsetRow(arrived, schema);
            
            return key;
        }
    };
    
    @SuppressWarnings("serial")
    private static class JoinExistingForKeysFunction
    implements PairFlatMapFunction<Iterator<Tuple2<Row, Iterable<Row>>>, Row, Tuple2<Iterable<Row>, Iterable<Row>>> {
        private Config outputConfig;
        private RandomReadWriteOutput output;
        private List<String> keyFieldNames;
        
        public JoinExistingForKeysFunction(Config outputConfig, List<String> keyFieldNames) {
            this.outputConfig = outputConfig;
            this.keyFieldNames = keyFieldNames;
        }
        
        @Override
        public Iterable<Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>>>
        call(Iterator<Tuple2<Row, Iterable<Row>>> arrivingForKeysIterator) throws Exception
        {
            if (!arrivingForKeysIterator.hasNext()) {
                return Lists.newArrayList();
            }
            
            if (output == null) {
                output = (RandomReadWriteOutput)Output.outputFor(outputConfig);
            }
            
            List<Tuple2<Row, Iterable<Row>>> arrivingForKeys = Lists.newArrayList(arrivingForKeysIterator);
            
            Set<Row> arrivingKeys = extractKeys(arrivingForKeys);
            
            Iterable<Row> existingWithoutKeys = output.getExistingForFilters(arrivingKeys);
            Map<Row, Iterable<Row>> existingForKeys = mapExistingToKeys(existingWithoutKeys);
            
            List<Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>>> arrivingAndExistingForKeys = 
                    attachExistingToArrivingForKeys(existingForKeys, arrivingForKeys);
            
            return arrivingAndExistingForKeys;
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
            ExtractKeyFunction extractKeyFunction = new ExtractKeyFunction(keyFieldNames);
            
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
        private RandomReadWritePlanner planner;
        
        public PlanForKeyFunction(Config config) {
            this.config = config;
        }
        
        @Override
        public Iterable<PlannedRow>
        call(Tuple2<Row, Tuple2<Iterable<Row>, Iterable<Row>>> keyedRecords) throws Exception {
            if (planner == null) {
                planner = (RandomReadWritePlanner)Planner.plannerFor(config);
            }
            
            Row key = keyedRecords._1();
            List<Row> arrivingRecords = Lists.newArrayList(keyedRecords._2()._1());
            List<Row> existingRecords = Lists.newArrayList(keyedRecords._2()._2());
            
            Iterable<PlannedRow> plannedForKey = planner.planMutationsForKey(key, arrivingRecords, existingRecords);
            
            return plannedForKey;
        }
    };
    
    private void applyMutations(JavaRDD<PlannedRow> planned, Config outputConfig) {
        planned.foreachPartition(new ApplyMutationsForPartitionFunction(outputConfig));
    }
    
    @SuppressWarnings("serial")
    private static class ApplyMutationsForPartitionFunction implements VoidFunction<Iterator<PlannedRow>> {
        private Config config;
        private RandomWriteOutput output;
        
        public ApplyMutationsForPartitionFunction(Config config) {
            this.config = config;
        }
        
        @Override
        public void call(Iterator<PlannedRow> t) throws Exception {
            if (output == null) {
                output = (RandomWriteOutput)Output.outputFor(config);
            }
            
            output.applyMutations(Lists.newArrayList(t));
        }
    }

}
