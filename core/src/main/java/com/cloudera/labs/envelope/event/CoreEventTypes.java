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

package com.cloudera.labs.envelope.event;

import com.google.common.collect.Sets;

import java.util.Set;

public class CoreEventTypes {

  // The pipeline has started
  public static final String PIPELINE_STARTED =
      "envelope.pipeline.started";

  // The pipeline has finished without any exception
  public static final String PIPELINE_FINISHED =
      "envelope.pipeline.finished";

  // The pipeline has failed because of a propagated exception
  public static final String PIPELINE_EXCEPTION_OCCURRED =
      "envelope.pipeline.exception.occurred";

  // The steps have been instantiated from the pipeline configuration
  public static final String STEPS_EXTRACTED =
      "envelope.steps.extracted";

  // The execution mode for the pipeline (e.g. batch, streaming) has been determined
  public static final String EXECUTION_MODE_DETERMINED =
      "envelope.execution.mode.determined";

  // The data step has written its data to its output
  public static final String DATA_STEP_WRITTEN_TO_OUTPUT =
      "envelope.data.step.written.to.output";

  // The data step has generated its data from its input or its deriver.
  // Note that when handling this event Spark is forced to execute steps one at a time, which
  // can lead to slower performance because Spark can not merge steps together. Good citizen
  // event handlers should allow users to optionally ignore this event for best performance.
  public static final String DATA_STEP_DATA_GENERATED =
      "envelope.data.step.data.generated";

  public static Set<String> getAllCoreEventTypes() {
    return Sets.newHashSet(
        PIPELINE_STARTED,
        PIPELINE_FINISHED,
        PIPELINE_EXCEPTION_OCCURRED,
        STEPS_EXTRACTED,
        EXECUTION_MODE_DETERMINED,
        DATA_STEP_WRITTEN_TO_OUTPUT,
        DATA_STEP_DATA_GENERATED
    );
  }

  public static Set<String> getHighPerformanceImpactCoreEventTypes() {
    return Sets.newHashSet(
        DATA_STEP_DATA_GENERATED
    );
  }

}
