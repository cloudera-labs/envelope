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

package com.cloudera.labs.envelope.event;

public class CoreEventMetadataKeys {

  public static final String STEPS_EXTRACTED_CONFIG =
      CoreEventTypes.STEPS_EXTRACTED + ".config";

  public static final String STEPS_EXTRACTED_STEPS =
      CoreEventTypes.STEPS_EXTRACTED + ".steps";

  public static final String STEPS_EXTRACTED_TIME_TAKEN_NS =
      CoreEventTypes.STEPS_EXTRACTED + ".time.taken.ns";

  public static final String DATA_STEP_DATA_GENERATED_STEP_NAME =
      CoreEventTypes.DATA_STEP_DATA_GENERATED + ".step.name";

  public static final String DATA_STEP_DATA_GENERATED_ROW_COUNT =
      CoreEventTypes.DATA_STEP_DATA_GENERATED + ".row.count";

  public static final String DATA_STEP_DATA_GENERATED_TIME_TAKEN_NS =
      CoreEventTypes.DATA_STEP_DATA_GENERATED + ".time.taken.ns";

  public static final String DATA_STEP_WRITTEN_TO_OUTPUT_STEP_NAME =
      CoreEventTypes.DATA_STEP_WRITTEN_TO_OUTPUT + ".step.name";

  public static final String DATA_STEP_WRITTEN_TO_OUTPUT_TIME_TAKEN_NS =
      CoreEventTypes.DATA_STEP_WRITTEN_TO_OUTPUT + ".time.taken.ns";

  public static final String PIPELINE_EXCEPTION_OCCURRED_EXCEPTION =
      CoreEventTypes.PIPELINE_EXCEPTION_OCCURRED + ".exception";

  public static final String EXECUTION_MODE_DETERMINED_MODE =
      CoreEventTypes.EXECUTION_MODE_DETERMINED + ".mode";

}
