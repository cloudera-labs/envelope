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

import com.cloudera.labs.envelope.output.OutputFactory;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.run.TestRunner;
import com.cloudera.labs.envelope.validate.ValidationAssert;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestOutputEventHandler {

  @Test
  public void testOutputEventHandler() {
    Map<String, Object> configMap = Maps.newHashMap();
    configMap.put(OutputEventHandler.LOG_ALL_EVENTS_CONFIG, true);
    configMap.put(
        OutputEventHandler.OUTPUT_CONFIG + "." + OutputFactory.TYPE_CONFIG_NAME,
        TestRunner.TestingMemoryOutput.class.getName());
    Config config = ConfigFactory.parseMap(configMap);

    OutputEventHandler eh = new OutputEventHandler();
    ValidationAssert.assertNoValidationFailures(eh, config);
    eh.configure(config);

    TestRunner.TestingMemoryOutput.reset();
    EventManager.reset();
    EventManager.register(Collections.<EventHandler>singleton(eh));

    eh.handle(new Event("hello", "world"));

    List<Row> rows = TestRunner.TestingMemoryOutput.getRows();

    assertEquals(1, rows.size());
    Row row = rows.get(0);

    assertTrue(row.getString(row.fieldIndex(OutputEventHandler.EVENT_ID_FIELD)).length() > 0);
    assertNotNull(row.getTimestamp(row.fieldIndex(OutputEventHandler.TIMESTAMP_UTC_FIELD)));
    assertEquals(row.getAs(OutputEventHandler.EVENT_TYPE_FIELD), "hello");
    assertEquals(row.getAs(OutputEventHandler.MESSAGE_FIELD), "world");
    assertTrue(row.getString(row.fieldIndex(OutputEventHandler.PIPELINE_ID_FIELD)).length() > 0);
    assertEquals(row.getString(row.fieldIndex(MutationType.MUTATION_TYPE_FIELD_NAME)),
        MutationType.INSERT.toString());
  }

}
