/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.derive;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.typesafe.config.Config;

/**
 * <p>Returns new dataset containing only unique rows from the dataset specified in "dependencies".</p>
 * <p>If "dependencies" is a list, a <code>step</code> config parameter is required to disambiguate operand for distinct() operation. 
 */

public class DistinctDeriver implements Deriver, ProvidesAlias {

	  public static final String DISTINCT_STEP_CONFIG = "step";

	  private String stepName = null ;

  @Override
  public void configure(Config config) {
	  if (config.hasPath(DISTINCT_STEP_CONFIG)) {
		  stepName = config.getString(DISTINCT_STEP_CONFIG);
	  }
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
	  validate(dependencies) ;
	  Dataset<Row> temp = dependencies.get(stepName) ;
	  return temp.distinct() ;
  }

  @Override
  public String getAlias() {
    return "distinct";
  }

  private void validate(Map<String, Dataset<Row>> dependencies) {
	  switch( dependencies.size() ) {
	  case 0:
		  throw new RuntimeException("Distinct deriver requires at least one dependency");
	  case 1:
		  if (stepName==null || stepName.trim().length()==0) {
			  stepName = dependencies.keySet().toArray(new String[0])[0] ;
		  }
		  else {
			  if (!dependencies.containsKey(stepName)) {
				  String cause = "Invalid \"step\" configuration: "+stepName+" is not a dependency: "+dependencies.keySet()+"" ;
				  throw new RuntimeException(cause);
			  }
		  }
		  break ;
	  default: 
		  if (stepName==null || stepName.trim().length()==0)
			  throw new RuntimeException("Distinct deriver requires a \"step\" configuration when multiple dependencies have been listed: "+dependencies.keySet()+"");
		  if (!dependencies.containsKey(stepName)) 
			  throw new RuntimeException("Invalid \"step\" configuration: "+stepName+" is not listed as dependency: "+dependencies.keySet()+"");
	  }
  }
  
}
