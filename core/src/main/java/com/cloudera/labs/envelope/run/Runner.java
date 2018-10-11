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

import static com.cloudera.labs.envelope.security.SecurityUtils.SECURITY_PREFIX;
import static com.cloudera.labs.envelope.spark.Contexts.APPLICATION_SECTION_PREFIX;

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.input.BatchInput;
import com.cloudera.labs.envelope.input.Input;
import com.cloudera.labs.envelope.input.InputFactory;
import com.cloudera.labs.envelope.input.StreamInput;
import com.cloudera.labs.envelope.security.SecurityUtils;
import com.cloudera.labs.envelope.security.TokenStoreManager;
import com.cloudera.labs.envelope.security.UsesDelegationTokens;
import com.cloudera.labs.envelope.spark.AccumulatorRequest;
import com.cloudera.labs.envelope.spark.Accumulators;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.spark.Contexts.ExecutionMode;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.StepUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.ValidationResult;
import com.cloudera.labs.envelope.validate.ValidationUtils;
import com.cloudera.labs.envelope.validate.Validations;
import com.cloudera.labs.envelope.validate.Validator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runner merely submits the pipeline steps to Spark in dependency order.
 * Ultimately the DAG scheduling is being coordinated by Spark, not Envelope.
 */
public class Runner {

  public static final String STEPS_SECTION_CONFIG = "steps";
  public static final String TYPE_PROPERTY = "type";
  public static final String DATA_TYPE = "data";
  public static final String LOOP_TYPE = "loop";
  public static final String DECISION_TYPE = "decision";
  public static final String TASK_TYPE = "task";
  public static final String UDFS_SECTION_CONFIG = "udfs";
  public static final String UDFS_NAME = "name";
  public static final String UDFS_CLASS = "class";
  public static final String PIPELINE_THREADS_PROPERTY = "application.pipeline.threads";

  private static ExecutorService threadPool;
  private static TokenStoreManager tokenStoreManager;
  private static Logger LOG = LoggerFactory.getLogger(Runner.class);

  /**
   * Run the Envelope pipeline
   * @param config The full configuration of the Envelope pipeline
   */
  public static void run(Config config) throws Exception {
    validateConfigurations(config);

    Set<Step> steps = extractSteps(config, true);
    LOG.info("Steps instantiated");

    ExecutionMode mode = StepUtils.hasStreamingStep(steps) ? Contexts.ExecutionMode.STREAMING : Contexts.ExecutionMode.BATCH;
    Contexts.initialize(config, mode);

    initializeSecurity(config, steps);

    initializeAccumulators(steps);
    
    initializeUDFs(config);

    initializeThreadPool(config);

    try {
      if (StepUtils.hasStreamingStep(steps)) {
        LOG.debug("Streaming step(s) identified");

        runStreaming(steps);
      } else {
        LOG.debug("No streaming steps identified");

        runBatch(steps);
      }
    }
    finally {
      shutdownThreadPool();
      shutdownSecurity();
    }

    LOG.debug("Runner finished");
  }
  
  private static Set<Step> extractSteps(Config config, boolean configure) {
    LOG.debug("Starting getting steps");

    Set<Step> steps = Sets.newHashSet();

    Set<String> stepNames = config.getObject(STEPS_SECTION_CONFIG).keySet();
    for (String stepName : stepNames) {
      Config stepConfig = config.getConfig(STEPS_SECTION_CONFIG).getConfig(stepName);

      Step step;

      if (!stepConfig.hasPath(TYPE_PROPERTY) || stepConfig.getString(TYPE_PROPERTY).equals(DATA_TYPE)) {
        if (stepConfig.hasPath(DataStep.INPUT_TYPE)) {
          Config stepInputConfig = stepConfig.getConfig(DataStep.INPUT_TYPE);
          Input stepInput = InputFactory.create(stepInputConfig, false);

          if (stepInput instanceof BatchInput) {
            LOG.debug("Adding batch step: " + stepName);
            step = new BatchStep(stepName);
          }
          else if (stepInput instanceof StreamInput) {
            LOG.debug("Adding streaming step: " + stepName);
            step = new StreamingStep(stepName);
          }
          else {
            throw new RuntimeException("Invalid step input sub-class for: " + stepName);
          }
        }
        else {
          LOG.debug("Adding batch step: " + stepName);
          step = new BatchStep(stepName);
        }
      }
      else if (stepConfig.getString(TYPE_PROPERTY).equals(LOOP_TYPE)) {
        LOG.debug("Adding loop step: " + stepName);
        step = new LoopStep(stepName);
      }
      else if (stepConfig.getString(TYPE_PROPERTY).equals(TASK_TYPE)) {
        LOG.debug("Adding task step: " + stepName);
        step = new TaskStep(stepName);
      }
      else if (stepConfig.getString(TYPE_PROPERTY).equals(DECISION_TYPE)) {
        LOG.debug("Adding decision step: " + stepName);
        step = new DecisionStep(stepName);
      }
      else {
        throw new RuntimeException("Unknown step type: " + stepConfig.getString(TYPE_PROPERTY));
      }

      if (configure) {
        step.configure(stepConfig);
        LOG.debug("With configuration: " + stepConfig);
      }

      steps.add(step);
    }

    LOG.debug("Finished getting steps");

    return steps;
  }

  /**
   * Run the Envelope pipeline as a Spark Streaming job.
   * @param steps The full configuration of the Envelope pipeline
   */
  @SuppressWarnings("unchecked")
  private static void runStreaming(final Set<Step> steps) throws Exception {
    final Set<Step> independentNonStreamingSteps = StepUtils.getIndependentNonStreamingSteps(steps);
    runBatch(independentNonStreamingSteps);

    Set<StreamingStep> streamingSteps = StepUtils.getStreamingSteps(steps);
    for (final StreamingStep streamingStep : streamingSteps) {
      LOG.debug("Setting up streaming step: " + streamingStep.getName());

      @SuppressWarnings("rawtypes")
      JavaDStream stream = streamingStep.getStream();

      final StructType streamSchema = streamingStep.getSchema();
      LOG.debug("Stream schema: " + streamSchema);

      stream.foreachRDD(new VoidFunction<JavaRDD<?>>() {
        @Override
        public void call(JavaRDD<?> raw) throws Exception {
          // Some independent steps might be repeating steps that have been flagged for reload
          StepUtils.resetRepeatingSteps(steps);
          // This will run any batch steps (and dependents) that are not submitted
          runBatch(independentNonStreamingSteps);

          JavaRDD<Row> translated = streamingStep.translate(raw);
          
          Dataset<Row> batchDF = Contexts.getSparkSession().createDataFrame(translated, streamSchema);
          streamingStep.setData(batchDF);
          streamingStep.setSubmitted(true);

          Set<Step> dependentSteps = StepUtils.getAllDependentSteps(streamingStep, steps);
          Set<Step> batchSteps = Sets.newHashSet(dependentSteps);
          batchSteps.add(streamingStep);
          batchSteps.addAll(independentNonStreamingSteps);
          runBatch(batchSteps);

          StepUtils.resetSteps(dependentSteps);
          
          streamingStep.recordProgress(raw);
        }
      });

      LOG.debug("Finished setting up streaming step: " + streamingStep.getName());
    }

    JavaStreamingContext jsc = Contexts.getJavaStreamingContext();
    jsc.start();
    LOG.debug("Streaming context started");
    jsc.awaitTermination();
    LOG.debug("Streaming context terminated");
  }

  /**
   * Run the steps in dependency order.
   * @param steps The steps to run, which may be the full Envelope pipeline, or a subset of it.
   */
  private static void runBatch(Set<Step> steps) throws Exception {
    LOG.debug("Started batch for steps: {}", StepUtils.stepNamesAsString(steps));
    
    Set<Future<Void>> offMainThreadSteps = Sets.newHashSet();
    Set<Step> refactoredSteps = null;

    // The essential logic is to loop through all of the steps until they have all been submitted.
    // Steps will not be submitted until all of their dependency steps have been submitted first.
    while (!StepUtils.allStepsSubmitted(steps)) {
      LOG.debug("Not all steps have been submitted");
      
      for (final Step step : steps) {
        LOG.debug("Looking into step: " + step.getName());

        if (step instanceof BatchStep) {
          LOG.debug("Step is batch");
          BatchStep batchStep = (BatchStep)step;

          if (!batchStep.hasSubmitted()) {
            LOG.debug("Step has not been submitted");

            // Get the dependency steps that exist so far. Steps can be created
            // during runtime by refactor steps, so this set may be smaller than
            // the full list of dependencies the step needs to wait for.
            final Set<Step> dependencies = StepUtils.getDependencies(step, steps);

            if (dependencies.size() == step.getDependencyNames().size() &&
                StepUtils.allStepsSubmitted(dependencies)) {
              LOG.debug("Step dependencies have been submitted, running step off main thread");
              // Batch steps are run off the main thread so that if they contain outputs they will
              // not block the parallel execution of independent steps.
              Future<Void> offMainThreadStep = runStepOffMainThread(batchStep, dependencies, threadPool);
              offMainThreadSteps.add(offMainThreadStep);
            }
            else {
              LOG.debug("Step dependencies have not been submitted");
            }
          }
          else {
            LOG.debug("Step has been submitted");
          }
        }
        else if (step instanceof StreamingStep) {
          LOG.debug("Step is streaming");
        }
        else if (step instanceof RefactorStep) {
          LOG.debug("Step is a refactor step");
          
          RefactorStep refactorStep = (RefactorStep)step;
          
          if (!refactorStep.hasSubmitted()) {
            LOG.debug("Step has not been submitted");
          
            final Set<Step> dependencies = StepUtils.getDependencies(step, steps);
  
            if (StepUtils.allStepsSubmitted(dependencies)) {
              LOG.debug("Step dependencies have submitted, refactoring steps");
              refactoredSteps = refactorStep.refactor(steps);
              LOG.debug("Steps refactored");
              break;
            }
            else {
              LOG.debug("Step dependencies have not been submitted");
            }
          }
          else {
            LOG.debug("Step has been submitted");
          }
        }
        else if (step instanceof TaskStep) {
          LOG.debug("Step is a task");
          
          TaskStep taskStep = (TaskStep)step;
          
          if (!taskStep.hasSubmitted()) {
            LOG.debug("Step has not been submitted");
          
            final Set<Step> dependencies = StepUtils.getDependencies(step, steps);
            
            if (StepUtils.allStepsSubmitted(dependencies)) {
              LOG.debug("Step dependencies have finished, running task");
              taskStep.run(StepUtils.getStepDataFrames(dependencies));
              LOG.debug("Task finished");
            }
            else {
              LOG.debug("Step dependencies have not been submitted");
            }
          }
          else {
            LOG.debug("Step has been submitted");
          }  
        }
        else {
          throw new RuntimeException("Unknown step class type: " + step.getClass().getName());
        }

        LOG.debug("Finished looking into step: " + step.getName());
      }

      awaitAllOffMainThreadsFinished(offMainThreadSteps);
      offMainThreadSteps.clear();
      
      if (refactoredSteps != null) {
        steps = refactoredSteps;
        refactoredSteps = null;
      }
    }

    LOG.debug("Finished batch for steps: {}", StepUtils.stepNamesAsString(steps));
  }

  private static void initializeThreadPool(Config config) {
    if (config.hasPath(PIPELINE_THREADS_PROPERTY)) {
      threadPool = Executors.newFixedThreadPool(config.getInt(PIPELINE_THREADS_PROPERTY));
    }
    else {
      threadPool = Executors.newFixedThreadPool(20);
    }
  }

  private static Future<Void> runStepOffMainThread(final BatchStep step, final Set<Step> dependencies, final ExecutorService threadPool) {
    return threadPool.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        step.submit(dependencies);
        return null;
      }
    });
  }

  private static void awaitAllOffMainThreadsFinished(Set<Future<Void>> offMainThreadSteps) throws Exception {
    for (Future<Void> offMainThreadStep : offMainThreadSteps) {
      offMainThreadStep.get();
    }
  }

  private static void shutdownThreadPool() {
    threadPool.shutdown();
  }

  private static void validateConfigurations(Config config) {
    if (ConfigUtils.getOrElse(config, Validator.CONFIGURATION_VALIDATION_ENABLED_PROPERTY,
        !Validator.CONFIGURATION_VALIDATION_ENABLED_DEFAULT))
    {
      LOG.info("Envelope configuration validation disabled");
      return;
    }

    LOG.info("Validating provided Envelope configuration");

    List<ValidationResult> results = Lists.newArrayList();

    // Validate steps
    Set<Step> steps;
    try {
      steps = extractSteps(config, false);
    }
    catch (Exception e) {
      if (e.getCause() instanceof ClassNotFoundException) {
        throw new RuntimeException("Could not find an input class that was specified in the pipeline. " +
            "A common cause for this is when the jar file for a plugin is not provided on the" +
            "classpath using --jars.", e.getCause());
      } else {
        throw new RuntimeException("Could not create steps from configuration file", e);
      }
    }
    for (Step step : steps) {
      List<ValidationResult> stepResults =
          Validator.validate(step, config.getConfig(STEPS_SECTION_CONFIG).getConfig(step.getName()));
      ValidationUtils.prefixValidationResultMessages(stepResults, "Step '" + step.getName() + "'");
      results.addAll(stepResults);
    }

    // Validate application section
    Config applicationConfig = config.hasPath(Contexts.APPLICATION_SECTION_PREFIX) ?
        config.getConfig(Contexts.APPLICATION_SECTION_PREFIX) : ConfigFactory.empty();
    List<ValidationResult> applicationResults = Validator.validate(new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder()
            .optionalPath(Contexts.APPLICATION_NAME_PROPERTY, ConfigValueType.STRING)
            .optionalPath(Contexts.NUM_EXECUTORS_PROPERTY, ConfigValueType.NUMBER)
            .optionalPath(Contexts.NUM_EXECUTOR_CORES_PROPERTY, ConfigValueType.NUMBER)
            .optionalPath(Contexts.EXECUTOR_MEMORY_PROPERTY, ConfigValueType.STRING)
            .optionalPath(Contexts.BATCH_MILLISECONDS_PROPERTY, ConfigValueType.NUMBER)
            .optionalPath(PIPELINE_THREADS_PROPERTY, ConfigValueType.NUMBER)
            .optionalPath(Contexts.SPARK_SESSION_ENABLE_HIVE_SUPPORT, ConfigValueType.BOOLEAN)
            .handlesOwnValidationPath(Contexts.SPARK_CONF_PROPERTY_PREFIX)
            .build();
      }
    }, applicationConfig);
    ValidationUtils.prefixValidationResultMessages(applicationResults, "Application");
    results.addAll(applicationResults);

    // Validate UDFs are provided as a list
    // TODO: inspect UDF objects to see if they are each valid
    results.addAll(Validator.validate(new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder()
            .optionalPath(UDFS_SECTION_CONFIG, ConfigValueType.LIST)
            .allowUnrecognizedPaths()
            .build();
      }
    }, config));

    ValidationUtils.logValidationResults(results);

    if (ValidationUtils.hasValidationFailures(results)) {
      LOG.error("Provided Envelope configuration did not pass all validation checks. " +
          "See above for information on each failed check. " +
          "Configuration validation can be disabled with '" +
          Validator.CONFIGURATION_VALIDATION_ENABLED_PROPERTY + " = false' either at the top-level " +
          "or within any validated scope of the configuration file. " +
          "Configuration documentation can be found at " +
          "https://github.com/cloudera-labs/envelope/blob/master/docs/configurations.adoc");
      System.exit(1);
    }

    LOG.info("Provided Envelope configuration is valid (" + results.size() + " checks passed)");
  }

  private static void initializeSecurity(Config config, Set<Step> steps) throws Exception {
    tokenStoreManager = new TokenStoreManager(ConfigUtils.getOrElse(config,
        APPLICATION_SECTION_PREFIX + "." + SECURITY_PREFIX, ConfigFactory.empty()));
    LOG.info("Security manager created");

    Set<InstantiatedComponent> secureComponents = Sets.newHashSet();

    // Get all security providers
    for (Step step : steps) {
      if (step instanceof InstantiatesComponents) {
        InstantiatesComponents thisStep = (InstantiatesComponents) step;
        // Check for secure components - components have already been configured at this stage
        Set<InstantiatedComponent> components = thisStep.getComponents(step.getConfig(), true);

        for (InstantiatedComponent instantiatedComponent : components) {
          secureComponents.addAll(SecurityUtils.getAllSecureComponents(instantiatedComponent));
        }
      }
    }

    // Get all token providers
    for (InstantiatedComponent secureComponent : secureComponents) {
      tokenStoreManager.addTokenProvider(((UsesDelegationTokens)secureComponent.getComponent()).getTokenProvider());
    }

    LOG.debug("Starting TokenStoreManager thread");
    tokenStoreManager.start();
  }

  private static void shutdownSecurity() {
    tokenStoreManager.stop();
  }
  
  private static void initializeAccumulators(Set<Step> steps) {
    Set<AccumulatorRequest> requests = Sets.newHashSet();
    
    for (DataStep dataStep : StepUtils.getDataSteps(steps)) {
      requests.addAll(dataStep.getAccumulatorRequests());
    }
    
    Accumulators accumulators = new Accumulators(requests);
    
    for (DataStep dataStep : StepUtils.getDataSteps(steps)) {
      dataStep.receiveAccumulators(accumulators);
    }
  }
  
  static void initializeUDFs(Config config) {
    if (!config.hasPath(UDFS_SECTION_CONFIG)) return;
    
    ConfigList udfList = config.getList(UDFS_SECTION_CONFIG);
    
    for (ConfigValue udfValue : udfList) {
      ConfigValueType udfValueType = udfValue.valueType();
      if (!udfValueType.equals(ConfigValueType.OBJECT)) {
        throw new RuntimeException("UDF list must contain UDF objects");
      }
      
      Config udfConfig = ((ConfigObject)udfValue).toConfig();
      
      for (String path : Lists.newArrayList(UDFS_NAME, UDFS_CLASS)) {
        if (!udfConfig.hasPath(path)) {
          throw new RuntimeException("UDF entries must provide '" + path + "'");
        }
      }
      
      String name = udfConfig.getString(UDFS_NAME);
      String className = udfConfig.getString(UDFS_CLASS);
      
      // null third argument means that registerJava will infer the return type
      Contexts.getSparkSession().udf().registerJava(name, className, null);
      
      LOG.info("Registered Spark SQL UDF: " + name);
    }
  }

}
