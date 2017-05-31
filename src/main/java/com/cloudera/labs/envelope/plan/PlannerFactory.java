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
package com.cloudera.labs.envelope.plan;

import java.lang.reflect.Constructor;

import com.typesafe.config.Config;

public class PlannerFactory {

  public static final String TYPE_CONFIG_NAME = "type";

  public static Planner create(Config plannerConfig) throws Exception {
    if (!plannerConfig.hasPath(TYPE_CONFIG_NAME)) {
      throw new RuntimeException("Planner type not specified");
    }

    String plannerType = plannerConfig.getString(TYPE_CONFIG_NAME);

    Planner planner;

    switch (plannerType) {
      case "append":
        planner = new AppendPlanner();
        break;
      case "upsert":
        planner = new SystemTimeUpsertPlanner();
        break;
      case "overwrite":
        planner = new OverwritePlanner();
        break;
      case "eventtimeupsert":
        planner = new EventTimeUpsertPlanner();
        break;
      case "history":
        planner = new EventTimeHistoryPlanner();
        break;
      case "bitemporal":
        planner = new BitemporalHistoryPlanner();
        break;
      case "delete":
        planner = new DeletePlanner();
        break;
      default:
        Class<?> clazz = Class.forName(plannerType);
        Constructor<?> constructor = clazz.getConstructor();
        planner = (Planner)constructor.newInstance();
    }

    planner.configure(plannerConfig);

    return planner;
  }

}
