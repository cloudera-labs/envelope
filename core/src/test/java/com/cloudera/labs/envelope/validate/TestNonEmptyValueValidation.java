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

package com.cloudera.labs.envelope.validate;

import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestNonEmptyValueValidation {

  @Test
  public void testNotEmptyString() {
    Validation v = new NonEmptyValueValidation("hello");
    ValidationResult vr = v.validate(ConfigFactory.parseString("hello = \"world\""));
    assertEquals(vr.getValidity(), Validity.VALID);
  }

  @Test
  public void testEmptyString() {
    Validation v = new NonEmptyValueValidation("hello");
    ValidationResult vr = v.validate(ConfigFactory.parseString("hello = \"\""));
    assertEquals(vr.getValidity(), Validity.INVALID);
  }

  @Test
  public void testNotEmptyList() {
    Validation v = new NonEmptyValueValidation("test");
    ValidationResult vr = v.validate(ConfigFactory.parseString("test = [\"hello\", \"world\"]"));
    assertEquals(vr.getValidity(), Validity.VALID);
  }

  @Test
  public void testEmptyList() {
    Validation v = new NonEmptyValueValidation("test");
    ValidationResult vr = v.validate(ConfigFactory.parseString("test = []"));
    assertEquals(vr.getValidity(), Validity.INVALID);
  }

  @Test
  public void testNull() {
    Validation v = new NonEmptyValueValidation("test");
    ValidationResult vr = v.validate(ConfigFactory.parseString("test = null"));
    assertEquals(vr.getValidity(), Validity.INVALID);
  }

  @Test
  public void testPathDoesntExist() {
    Validation v = new NonEmptyValueValidation("test");
    ValidationResult vr = v.validate(ConfigFactory.parseString("hello = \"world\""));
    assertEquals(vr.getValidity(), Validity.VALID);
  }

  @Test
  public void testKnownPaths() {
    Validation v = new NonEmptyValueValidation("hello");
    assertEquals(Sets.newHashSet("hello"), v.getKnownPaths());
  }

}
