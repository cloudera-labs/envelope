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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestEventUtils {

  @Test
  public void testPrettifyNs() {
    assertEquals("1ns", EventUtils.prettifyNs(1));
    assertEquals("100ns", EventUtils.prettifyNs(100));
    assertEquals("123ns", EventUtils.prettifyNs(123));
    assertEquals("999ns", EventUtils.prettifyNs(999));
    assertEquals("1us", EventUtils.prettifyNs(1000));
    assertEquals("100us", EventUtils.prettifyNs(100000));
    assertEquals("123.45us", EventUtils.prettifyNs(123456));
    assertEquals("999.99us", EventUtils.prettifyNs(999999));
    assertEquals("1ms", EventUtils.prettifyNs(1000000));
    assertEquals("100ms", EventUtils.prettifyNs(100000000));
    assertEquals("123.45ms", EventUtils.prettifyNs(123456789));
    assertEquals("999.99ms", EventUtils.prettifyNs(999999999));
    assertEquals("1s", EventUtils.prettifyNs(1000000000));
    assertEquals("100s", EventUtils.prettifyNs(100000000000L));
    assertEquals("123.45s", EventUtils.prettifyNs(123456789012L));
    assertEquals("999.99s", EventUtils.prettifyNs(999999999999L));
  }

  @Test
  public void testCallingClassName() {
    assertEquals(TestEventUtils.class.getName(), EventUtils.getCallingClassName());
  }

}
