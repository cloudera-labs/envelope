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

package com.cloudera.labs.envelope.run;

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.event.CoreEventMetadataKeys;
import com.cloudera.labs.envelope.event.CoreEventTypes;
import com.cloudera.labs.envelope.event.Event;
import com.cloudera.labs.envelope.event.EventHandler;
import com.cloudera.labs.envelope.event.EventManager;
import com.cloudera.labs.envelope.event.impl.LogEventHandler;
import com.cloudera.labs.envelope.security.SecurityUtils;
import com.cloudera.labs.envelope.security.TokenProvider;
import com.cloudera.labs.envelope.security.TokenStoreListener;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.cloudera.labs.envelope.security.SecurityUtils.SECURITY_PREFIX;

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
  public static final String EVENT_HANDLERS_CONFIG = "event-handlers";
  public static final String CONFIG_LOADER_PROPERTY = "config-loader";
  public static final String PIPELINE_THREADS_PROPERTY = "application.pipeline.threads";

  private Config baseConfig;
  private ExecutorService threadPool;
  private TokenStoreManager tokenStoreManager;

  private static Logger LOG = LoggerFactory.getLogger(Runner.class);

  /**
   * Run the Envelope pipeline
   * @param config The full configuration of the Envelope pipeline
   */
  public void run(Config config) throws Exception {
    this.baseConfig = config;
    config = ConfigUtils.mergeLoadedConfiguration(config);

    validateConfigurations(config);

    Set<Step> steps = StepUtils.extractSteps(config, true, true);

    ExecutionMode mode = getExecutionMode(steps);

    Contexts.initialize(config, mode);

    initializeSecurity(config, steps);

    initializeEventHandlers(config);

    initializeAccumulators(steps);

    initializeUDFs(config);

    initializeThreadPool(config);

    notifyPipelineStarted();

    try {
      if (mode == ExecutionMode.STREAMING) {
        runStreaming(steps);
      } else {
        runBatch(steps);
      }
    }
    catch (Exception e) {
      notifyPipelineException(e);
      throw e;
    }
    finally {
      shutdownThreadPool();
      shutdownSecurity();
    }

    notifyPipelineFinished();
  }

  private ExecutionMode getExecutionMode(Set<Step> steps) {
    ExecutionMode mode = StepUtils.hasStreamingStep(steps) ? Contexts.ExecutionMode.STREAMING : Contexts.ExecutionMode.BATCH;
    notifyExecutionMode(mode);

    return mode;
  }

  /**
   * Run the Envelope pipeline as a Spark Streaming job.
   * @param steps The full configuration of the Envelope pipeline
   */
  @SuppressWarnings("unchecked")
  private void runStreaming(final Set<Step> steps) throws Exception {
    final Set<Step> independentNonStreamingSteps = StepUtils.getIndependentNonStreamingSteps(steps);
    runBatch(independentNonStreamingSteps);

    Set<StreamingStep> streamingSteps = StepUtils.getStreamingSteps(steps);
    for (final StreamingStep streamingStep : streamingSteps) {
      LOG.debug("Setting up streaming step: " + streamingStep.getName());

      JavaDStream stream = streamingStep.getStream();

      stream.foreachRDD(new VoidFunction<JavaRDD<?>>() {
        @Override
        public void call(JavaRDD<?> raw) throws Exception {
          // Some independent steps might be repeating steps that have been flagged for reload
          StepUtils.resetRepeatingSteps(steps);
          // This will run any batch steps (and dependents) that are not submitted
          runBatch(independentNonStreamingSteps);

          streamingStep.setData(streamingStep.translate(raw));
          streamingStep.writeData();
          streamingStep.setState(StepState.FINISHED);

          Set<Step> batchSteps = StepUtils.mergeLoadedSteps(steps, streamingStep, baseConfig);
          Set<Step> dependentSteps = StepUtils.getAllDependentSteps(streamingStep, batchSteps);
          batchSteps.add(streamingStep);
          batchSteps.addAll(streamingStep.loadNewBatchSteps());
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
  private void runBatch(Set<Step> steps) throws Exception {
    if (steps.isEmpty()) {
      return;
    }

    LOG.debug("Started batch for steps: {}", StepUtils.stepNamesAsString(steps));

    Set<Future<Void>> offMainThreadSteps = Sets.newHashSet();
    Set<Step> refactoredSteps = null;
    Map<String, StepState> previousStepStates = null;

    // The essential logic is to loop through all of the steps until they have all been submitted.
    // Steps will not be submitted until all of their dependency steps have been submitted first.
    while (!StepUtils.allStepsSubmitted(steps)) {
      LOG.debug("Not all steps have been submitted");

      Set<Step> newSteps = Sets.newHashSet();
      for (final Step step : steps) {
        LOG.debug("Looking into step: " + step.getName());

        if (step instanceof BatchStep) {
          BatchStep batchStep = (BatchStep)step;

          if (batchStep.getState() == StepState.WAITING) {
            LOG.debug("Step has not been submitted");

            // Get the dependency steps that exist so far. Steps can be created
            // during runtime by refactor steps, so this set may be smaller than
            // the full list of dependencies the step needs to wait for.
            final Set<Step> dependencies = StepUtils.getDependencies(step, steps);

            if (dependencies.size() == step.getDependencyNames().size() &&
                StepUtils.allStepsFinished(dependencies)) {
              LOG.debug("Step dependencies have finished, running step off main thread");
              // Batch steps are run off the main thread so that if they contain outputs they will
              // not block the parallel execution of independent steps.
              batchStep.setState(StepState.SUBMITTED);
              Future<Void> offMainThreadStep = runStepOffMainThread(batchStep, dependencies, threadPool);
              offMainThreadSteps.add(offMainThreadStep);
            }
          }

          // If the step has created new batch data steps to load into the running batch,
          // retrieve those and add them in.
          newSteps.addAll(batchStep.loadNewBatchSteps());
        }
        else if (step instanceof RefactorStep) {
          RefactorStep refactorStep = (RefactorStep)step;
          
          if (refactorStep.getState() == StepState.WAITING) {
            LOG.debug("Step has not been submitted");

            final Set<Step> dependencies = StepUtils.getDependencies(step, steps);
  
            if (StepUtils.allStepsFinished(dependencies)) {
              LOG.debug("Step dependencies have finished, refactoring steps");
              refactorStep.setState(StepState.SUBMITTED);
              refactoredSteps = refactorStep.refactor(steps);
              LOG.debug("Steps refactored");
              break;
            }
          }
        }
        else if (step instanceof TaskStep) {
          TaskStep taskStep = (TaskStep)step;
          
          if (taskStep.getState() == StepState.WAITING) {
            LOG.debug("Step has not been submitted");

            final Set<Step> dependencies = StepUtils.getDependencies(step, steps);
            
            if (StepUtils.allStepsFinished(dependencies)) {
              LOG.debug("Step dependencies have finished, running task");
              taskStep.setState(StepState.SUBMITTED);
              taskStep.run(StepUtils.getStepDataFrames(dependencies));
              LOG.debug("Task finished");
            }
          }
        }

        LOG.debug("Finished looking into step: " + step.getName());
      }

      // Add all steps created while looping through previous set of steps.
      steps.addAll(newSteps);

      if (refactoredSteps != null) {
        steps = refactoredSteps;
        refactoredSteps = null;
      }

      // Make sure the loop doesn't get stuck from an incorrect config
      Map<String, StepState> stepStates = StepUtils.getStepStates(steps);
      Set<Step> waitingSteps = StepUtils.getStepsMatchingState(steps, StepState.WAITING);
      Set<Step> submittedSteps = StepUtils.getStepsMatchingState(steps, StepState.SUBMITTED);
      if (stepStates.equals(previousStepStates) &&
          waitingSteps.size() > 0 &&
          submittedSteps.size() == 0) {
        throw new RuntimeException("Envelope pipeline stuck due to steps waiting for dependencies " +
            "that do not exist. Steps: " + steps);
      }
      previousStepStates = stepStates;

      // Avoid the driver getting bogged down in checking for new steps to submit
      Thread.sleep(20);
    }

    // Wait for the submitted steps that haven't yet finished
    awaitAllOffMainThreadsFinished(offMainThreadSteps);

    LOG.debug("Finished batch for steps: {}", StepUtils.stepNamesAsString(steps));
  }

  private void initializeThreadPool(Config config) {
    if (config.hasPath(PIPELINE_THREADS_PROPERTY)) {
      threadPool = Executors.newFixedThreadPool(config.getInt(PIPELINE_THREADS_PROPERTY));
    }
    else {
      threadPool = Executors.newFixedThreadPool(20);
    }
  }

  private Future<Void> runStepOffMainThread(final BatchStep step, final Set<Step> dependencies, final ExecutorService threadPool) {
    return threadPool.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        step.submit(dependencies);
        return null;
      }
    });
  }

  private void awaitAllOffMainThreadsFinished(Set<Future<Void>> offMainThreadSteps) throws Exception {
    for (Future<Void> offMainThreadStep : offMainThreadSteps) {
      offMainThreadStep.get();
    }
  }

  private void shutdownThreadPool() {
    threadPool.shutdown();
  }

  private void initializeEventHandlers(Config config) {
    EventManager.register(getEventHandlers(config, true).values());
  }

  private Map<Config, EventHandler> getEventHandlers(Config config, boolean configure) {
    Map<Config, EventHandler> handlers = Maps.newHashMap();
    Set<String> nonConfiguredDefaultHandlerAliases = Sets.newHashSet(
        new LogEventHandler().getAlias()
    );

    if (ConfigUtils.getApplicationConfig(config).hasPath(EVENT_HANDLERS_CONFIG)) {
      List<? extends ConfigObject> handlerConfigObjects;
      try {
        handlerConfigObjects =
            ConfigUtils.getApplicationConfig(config).getObjectList(EVENT_HANDLERS_CONFIG);
      }
      catch (ConfigException.WrongType e) {
        throw new RuntimeException("Event handler configuration must be a list of event handler objects");
      }

      for (ConfigObject handlerConfigObject : handlerConfigObjects) {
        Config handlerConfig = handlerConfigObject.toConfig();
        EventHandler handler = ComponentFactory.create(EventHandler.class, handlerConfig, configure);
        handlers.put(handlerConfig, handler);

        // If this handler is a default handler then because it was configured we remove it from the
        // non-configured set. If this handler is not a default handler then this will be a no-op.
        nonConfiguredDefaultHandlerAliases.remove(handlerConfig.getString(ComponentFactory.TYPE_CONFIG_NAME));
      }
    }

    // Create the default handlers that were not already created above. These are created with
    // no configurations, so default handlers must only use optional configurations.
    for (String defaultHandlerAlias : nonConfiguredDefaultHandlerAliases) {
      Config defaultHandlerConfig = ConfigFactory.empty().withValue(
          ComponentFactory.TYPE_CONFIG_NAME, ConfigValueFactory.fromAnyRef(defaultHandlerAlias));
      EventHandler defaultHandler = ComponentFactory.create(EventHandler.class, defaultHandlerConfig, configure);
      handlers.put(defaultHandlerConfig, defaultHandler);
    }

    return handlers;
  }

  private void notifyPipelineStarted() {
    EventManager.notify(new Event(CoreEventTypes.PIPELINE_STARTED, "Pipeline started"));
  }

  private void notifyPipelineFinished() {
    EventManager.notify(new Event(CoreEventTypes.PIPELINE_FINISHED, "Pipeline finished"));
  }

  private void notifyPipelineException(Exception e) {
    Map<String, Object> metadata = Maps.newHashMap();
    metadata.put(CoreEventMetadataKeys.PIPELINE_EXCEPTION_OCCURRED_EXCEPTION, e);
    Event event = new Event(
        CoreEventTypes.PIPELINE_EXCEPTION_OCCURRED,
        "Pipeline exception occurred: " + e.getMessage(),
        metadata);
    EventManager.notify(event);
  }

  private void notifyExecutionMode(ExecutionMode mode) {
    Map<String, Object> metadata = Maps.newHashMap();
    metadata.put(CoreEventMetadataKeys.EXECUTION_MODE_DETERMINED_MODE, mode);

    EventManager.notify(new Event(
        CoreEventTypes.EXECUTION_MODE_DETERMINED,
        "Pipeline execution mode determined: " + mode,
        metadata));
  }

  private void validateConfigurations(Config config) {
    if (!ConfigUtils.getOrElse(config,
        Validator.CONFIGURATION_VALIDATION_ENABLED_PROPERTY,
        Validator.CONFIGURATION_VALIDATION_ENABLED_DEFAULT))
    {
      LOG.info("Envelope configuration validation disabled");
      return;
    }

    LOG.info("Validating provided Envelope configuration");

    List<ValidationResult> results = Lists.newArrayList();

    // Validate steps
    Set<Step> steps;
    try {
      steps = StepUtils.extractSteps(config, false, false);
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
    List<ValidationResult> applicationResults = Validator.validate(new ProvidesValidations() {
      @Override
      public Validations getValidations() {
        return Validations.builder()
            .optionalPath(Contexts.APPLICATION_NAME_PROPERTY, ConfigValueType.STRING)
            .optionalPath(Contexts.NUM_EXECUTORS_PROPERTY, ConfigValueType.NUMBER)
            .optionalPath(Contexts.NUM_EXECUTOR_CORES_PROPERTY, ConfigValueType.NUMBER)
            .optionalPath(Contexts.EXECUTOR_MEMORY_PROPERTY, ConfigValueType.STRING)
            .optionalPath(Contexts.BATCH_MILLISECONDS_PROPERTY, ConfigValueType.NUMBER)
            .optionalPath(EVENT_HANDLERS_CONFIG, ConfigValueType.LIST)
            .optionalPath(CONFIG_LOADER_PROPERTY, ConfigValueType.OBJECT)
            .optionalPath(PIPELINE_THREADS_PROPERTY, ConfigValueType.NUMBER)
            .optionalPath(Contexts.SPARK_SESSION_ENABLE_HIVE_SUPPORT, ConfigValueType.BOOLEAN)
            .handlesOwnValidationPath(Contexts.SPARK_CONF_PROPERTY_PREFIX)
            .handlesOwnValidationPath(CONFIG_LOADER_PROPERTY)
            .build();
      }
    }, ConfigUtils.getApplicationConfig(config));
    ValidationUtils.prefixValidationResultMessages(applicationResults, "Application");
    results.addAll(applicationResults);

    // Validate event handlers
    for (Map.Entry<Config, EventHandler> handlerWithConfig : getEventHandlers(config, false).entrySet()) {
      Config handlerConfig = handlerWithConfig.getKey();
      EventHandler handler = handlerWithConfig.getValue();
      if (handler instanceof ProvidesValidations) {
        List<ValidationResult> handlerResults = Validator.validate((ProvidesValidations)handler, handlerConfig);
        ValidationUtils.prefixValidationResultMessages(
            handlerResults, "Event Handler '" + handler.getClass().getSimpleName() + "'");
        results.addAll(handlerResults);
      }
    }

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

  private void initializeSecurity(Config config, Set<Step> steps) throws Exception {
    tokenStoreManager = new TokenStoreManager(ConfigUtils.getOrElse(
        ConfigUtils.getApplicationConfig(config), SECURITY_PREFIX, ConfigFactory.empty()));
    LOG.info("Security manager created");

    Set<InstantiatedComponent> secureComponents = Sets.newHashSet();

    // Get step security providers
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

    // Get event handler security providers
    for (EventHandler handler : getEventHandlers(config, true).values()) {
      if (handler instanceof InstantiatesComponents) {
        Set<InstantiatedComponent> components =
            ((InstantiatesComponents)handler).getComponents(config, true);

        for (InstantiatedComponent instantiatedComponent : components) {
          secureComponents.addAll(SecurityUtils.getAllSecureComponents(instantiatedComponent));
        }
      }
    }

    // Get all token providers
    for (InstantiatedComponent secureComponent : secureComponents) {
      TokenProvider tokenProvider = ((UsesDelegationTokens)secureComponent.getComponent()).getTokenProvider();
      if (tokenProvider != null) {
        tokenStoreManager.addTokenProvider(tokenProvider);
      }
    }

    LOG.debug("Starting TokenStoreManager thread");
    tokenStoreManager.start();
  }

  private void shutdownSecurity() {
    tokenStoreManager.stop();
    TokenStoreListener.stop();
  }

  private void initializeAccumulators(Set<Step> steps) {
    Set<AccumulatorRequest> requests = Sets.newHashSet();

    for (DataStep dataStep : StepUtils.getDataSteps(steps)) {
      requests.addAll(dataStep.getAccumulatorRequests());
    }

    Accumulators accumulators = new Accumulators(requests);

    for (DataStep dataStep : StepUtils.getDataSteps(steps)) {
      dataStep.receiveAccumulators(accumulators);
    }
  }

  void initializeUDFs(Config config) {
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
