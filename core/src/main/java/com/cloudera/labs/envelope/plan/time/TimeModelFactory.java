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

package com.cloudera.labs.envelope.plan.time;

import com.cloudera.labs.envelope.load.LoadableFactory;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import java.util.List;

public class TimeModelFactory extends LoadableFactory<TimeModel> {

  public static final String TYPE_CONFIG_NAME = "type";

  public static TimeModel create(Config timeModelConfig, String fieldName, boolean configure) {
    return create(timeModelConfig, Lists.newArrayList(fieldName), configure);
  }
  
  public static TimeModel create(Config timeModelConfig, List<String> fieldNames, boolean configure) {
    String timeModelType;
    if (timeModelConfig.hasPath(TYPE_CONFIG_NAME)) {
      timeModelType = timeModelConfig.getString(TYPE_CONFIG_NAME);
    }
    else {
      timeModelType = "longmillis";
    }
    
    TimeModel timeModel;
    try {
      timeModel = loadImplementation(TimeModel.class, timeModelType);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    if (configure) {
      timeModel.configure(timeModelConfig, fieldNames);
    }

    return timeModel;
  }
  
}
