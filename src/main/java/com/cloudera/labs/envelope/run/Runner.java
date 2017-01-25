package com.cloudera.labs.envelope.run;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.labs.envelope.input.BatchInput;
import com.cloudera.labs.envelope.input.Input;
import com.cloudera.labs.envelope.input.InputFactory;
import com.cloudera.labs.envelope.input.StreamInput;
import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

@SuppressWarnings("serial")
public class Runner {
    
    private static Logger LOG = LoggerFactory.getLogger(Runner.class);
    
    public static void run(Config config) throws Exception {
        Set<Step> steps = extractSteps(config);
        LOG.info("Steps instatiated");
        
        Contexts.initialize(config);
        
        if (hasStreamingStep(steps)) {
            LOG.info("Streaming step(s) identified");
            
            runStreaming(steps);
        }
        else {
            LOG.info("No streaming steps identified");
            
            runBatch(steps);
        }
        
        LOG.info("Runner finished");
    }
    
    private static void runStreaming(final Set<Step> steps) throws Exception {
        Set<Step> independentSteps = getIndependentSteps(steps);
        runBatch(independentSteps);
        
        Set<StreamingStep> streamingSteps = getStreamingSteps(steps);
        for (final StreamingStep streamingStep : streamingSteps) {
            LOG.info("Setting up streaming step: " + streamingStep.getName());
            
            JavaDStream<Row> stream = streamingStep.getStream();
            
            final StructType streamSchema = streamingStep.getSchema();
            LOG.info("Stream schema: " + streamSchema);
            
            stream.foreachRDD(new VoidFunction<JavaRDD<Row>>() {
                @Override
                public void call(JavaRDD<Row> batch) throws Exception {
                    DataFrame batchDF = Contexts.getSQLContext().createDataFrame(batch, streamSchema);
                    streamingStep.setData(batchDF);
                    streamingStep.setFinished(true);
                    
                    Set<Step> allDependentSteps = getAllDependentSteps(streamingStep, steps);
                    runBatch(allDependentSteps);
                    
                    resetDataSteps(allDependentSteps);
                };
            });
            
            LOG.info("Finished setting up streaming step: " + streamingStep.getName());
        }
        
        JavaStreamingContext sc = Contexts.getJavaStreamingContext();
        sc.start();
        LOG.info("Streaming context started");
        sc.awaitTermination();
        LOG.info("Streaming context terminated");
    }
    
    private static void runBatch(Set<? extends Step> steps) throws Exception {
        LOG.info("Started batch for steps: {}", stepNamesAsString(steps));
        
        ExecutorService threadPool = getNewThreadPool();
        Set<Future<Void>> offMainThreadSteps = Sets.newHashSet();
        
        while (!allDataStepsFinished(steps)) {
            LOG.info("Not all steps are finished");
            for (final Step step : steps) {
                LOG.info("Looking into step: " + step.getName());
                
                if (step instanceof BatchStep) {
                    LOG.info("Step is batch");
                    BatchStep batchStep = (BatchStep)step;
                    
                    if (!batchStep.hasFinished()) {
                        LOG.info("Step has not finished");
                        
                        final Set<Step> dependencies = getDependencies(step, steps);
                        
                        if (allDataStepsFinished(dependencies)) {
                            LOG.info("Step dependencies have finished, running step off main thread");
                            Future<Void> offMainThreadStep = runStepOffMainThread(batchStep, dependencies, threadPool);
                            offMainThreadSteps.add(offMainThreadStep);
                        }
                        else {
                            LOG.info("Step dependencies have not finished");
                        }
                    }
                    else {
                        LOG.info("Step has finished");
                    }
                }
                else {
                    LOG.info("Step is not batch");
                }
                
                LOG.info("Finished looking into step: " + step.getName());
            }
            
            awaitAllOffMainThreadsFinished(offMainThreadSteps);
            offMainThreadSteps.clear();
        }
        
        LOG.info("Finished batch for steps: {}", stepNamesAsString(steps));
    }
    
    private static Set<Step> extractSteps(Config config) throws Exception {
        LOG.info("Starting getting steps");
        
        Set<Step> steps = Sets.newHashSet();
        
        Set<String> stepNames = config.getObject("steps").keySet();
        for (String stepName : stepNames) {
            Config stepConfig = config.getConfig("steps").getConfig(stepName);
            
            Step step;
            
            if (!stepConfig.hasPath("type") || stepConfig.getString("type").equals("data")) {
                if (stepConfig.hasPath("input")) {
                    Config stepInputConfig = stepConfig.getConfig("input");
                    Input stepInput = InputFactory.create(stepInputConfig);
                    
                    if (stepInput instanceof BatchInput) {
                        LOG.info("Adding batch step: " + stepName);
                        step = new BatchStep(stepName, stepConfig);
                    }
                    else if (stepInput instanceof StreamInput) {
                        LOG.info("Adding streaming step: " + stepName);
                        step = new StreamingStep(stepName, stepConfig);
                    }
                    else {
                        throw new RuntimeException("Invalid step input sub-class for: " + stepName);
                    }
                }
                else {
                    LOG.info("Adding batch step: " + stepName);
                    step = new BatchStep(stepName, stepConfig);
                }
                
                LOG.info("With configuration: " + stepConfig);
            }
            else {
                throw new RuntimeException("Unknown step type: " + stepConfig.getString("type"));
            }
            
            steps.add(step);
        }
        
        LOG.info("Finished getting steps");
        
        return steps;
    }
    
    private static boolean allDataStepsFinished(Set<? extends Step> steps) {
        for (Step step : steps) {
            if (step instanceof DataStep) {
                if (!((DataStep)step).hasFinished()) {
                    return false;
                }
            }
        }
        
        return true;
    }
    
    private static Set<Step> getDependencies(Step step, Set<? extends Step> steps) {
        Set<Step> dependencies = Sets.newHashSet();
        
        Set<String> dependencyNames = step.getDependencyNames();
        for (Step candidate : steps) {
            String candidateName = candidate.getName();
            if (dependencyNames.contains(candidateName)) {
                dependencies.add(candidate);
            }
        }
        
        LOG.info("Dependencies of {} are: {}", step.getName(), stepNamesAsString(dependencies));
        
        return dependencies;
    }
    
    private static boolean hasStreamingStep(Set<Step> steps) {
        for (Step step : steps) {
            if (step instanceof StreamingStep) {
                return true;
            }
        }
        
        return false;
    }
    
    private static Set<StreamingStep> getStreamingSteps(Set<Step> steps) throws Exception {
        Set<StreamingStep> steamingSteps = Sets.newHashSet();
        
        for (Step step : steps) {
            if (step instanceof StreamingStep) {
                steamingSteps.add((StreamingStep)step);
            }
        }
        
        LOG.info("Streaming steps are: {}", stepNamesAsString(steamingSteps));
        
        return steamingSteps;
    }
    
    private static Set<Step> getAllDependentSteps(Step rootStep, Set<Step> steps) {
        Set<Step> dependencies = Sets.newHashSet();
        
        dependencies.add(rootStep);
        
        Set<BatchStep> immediateDependents = getImmediateDependentSteps(rootStep, steps);
        for (BatchStep immediateDependent : immediateDependents) {
            dependencies.addAll(getAllDependentSteps(immediateDependent, steps));
        }
        
        LOG.info("All dependent steps of {} are: {}", rootStep.getName(), stepNamesAsString(dependencies));
        
        return dependencies;
    }
    
    private static Set<BatchStep> getImmediateDependentSteps(Step step, Set<Step> steps) {
        Set<BatchStep> dependencies = Sets.newHashSet();
        
        for (Step candidateStep : steps) {
            if (candidateStep.getDependencyNames().contains(step.getName())) {
                dependencies.add((BatchStep)candidateStep);
            }
        }
        
        LOG.info("Immediate dependent steps of {} are: {}", step.getName(), stepNamesAsString(dependencies));
        
        return dependencies;
    }
    
    private static Set<Step> getIndependentSteps(Set<Step> steps) {
        Set<Step> independents = Sets.newHashSet();
        
        for (Step step : steps) {
            if (step instanceof BatchStep && step.getDependencyNames().isEmpty()) {
                independents.add((BatchStep)step);
            }
        }
        
        LOG.info("Independent steps are: {}", stepNamesAsString(independents));
        
        return independents;
    }
    
    private static String stepNamesAsString(Set<? extends Step> steps) {
        StringBuilder sb = new StringBuilder();
        
        for (Step step : steps) {
            sb.append(step.getName() + ", ");
        }
        
        if (sb.length() > 0) {
            sb.setLength(sb.length() - ", ".length());
        }
        
        return sb.toString();
    }
    
    private static void resetDataSteps(Set<Step> steps) {
        for (Step step : steps) {
            if (step instanceof DataStep) {
                ((DataStep)step).clearCache();
                ((DataStep)step).setFinished(false);
            }
        }
    }
    
    private static ExecutorService getNewThreadPool() {
        ExecutorService threadPool = Executors.newCachedThreadPool();
        
        return threadPool;
    }
    
    private static Future<Void> runStepOffMainThread(final BatchStep step, final Set<Step> dependencies, final ExecutorService threadPool) {
        return threadPool.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                step.runStep(dependencies);
                return null;
            }
        });
    }
    
    private static void awaitAllOffMainThreadsFinished(Set<Future<Void>> offMainThreadSteps) throws Exception {
        for (Future<Void> offMainThreadStep : offMainThreadSteps) {
            offMainThreadStep.get();
        }
    }
    
}
