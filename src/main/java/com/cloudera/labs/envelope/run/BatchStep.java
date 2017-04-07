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

import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.DataFrame;

import com.cloudera.labs.envelope.derive.PassthroughDeriver;
import com.cloudera.labs.envelope.input.BatchInput;
import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;

/**
 * A batch step is a data step that contains a single DataFrame.
 */
public class BatchStep extends DataStep {

  public BatchStep(String name, Config config) throws Exception {
    super(name, config);
  }

  public void runStep(Set<Step> dependencySteps) throws Exception {
    Contexts.getJavaSparkContext().sc().setJobDescription("Step: " + getName());

    DataFrame data;
    if (hasInput()) {
      data = ((BatchInput)input).read();
    }
    else if (hasDeriver()) {
      Map<String, DataFrame> dependencies = getStepDataFrames(dependencySteps);
      data = deriver.derive(dependencies);
    }
    else {
      deriver = new PassthroughDeriver();
      Map<String, DataFrame> dependencies = getStepDataFrames(dependencySteps);
      data = deriver.derive(dependencies);
    }

    setData(data);

    setFinished(true);
  }

}
