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

package com.cloudera.labs.envelope.component;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestInstantiatedComponent {

  @Test
  public void testEquality() {
    Component component = new Component(){};
    Config config = ConfigFactory.empty();
    String label = "hello";

    InstantiatedComponent ic1 = new InstantiatedComponent(component, config, label);
    InstantiatedComponent ic2 = new InstantiatedComponent(component, config, label);

    assertTrue(ic1.equals(ic2));
    assertEquals(ic1.hashCode(), ic2.hashCode());
  }

  @Test
  public void testInequality() {
    Component component = new Component(){};
    Config config = ConfigFactory.empty();
    String label1 = "hello";
    String label2 = "world";

    InstantiatedComponent ic1 = new InstantiatedComponent(component, config, label1);
    InstantiatedComponent ic2 = new InstantiatedComponent(component, config, label2);

    assertNotEquals(ic1, "different_class");
    assertNotEquals(ic1, null);

    assertFalse(ic1.equals(ic2));
    assertNotEquals(ic1.hashCode(), ic2.hashCode());
  }

}
