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

package com.cloudera.labs.envelope.validate;

import static org.junit.Assert.assertEquals;

import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class TestRegexValueValidation {

  @Test
  public void testValid() {
    RegexValueValidation v = new RegexValueValidation("query", "(?i)^refresh");
    ValidationResult vr = v.validate(ConfigFactory.parseString("query = refresh foobar"));
    assertEquals(vr.getValidity(), Validity.VALID);

    vr = v.validate(ConfigFactory.parseString("query = REFRESH db.foobar"));
    assertEquals(vr.getValidity(), Validity.VALID);
  }

  @Test
  public void testInvalid() {
    RegexValueValidation v = new RegexValueValidation("query", "(?i)^refresh ");
    ValidationResult vr = v.validate(ConfigFactory.parseString("query = refreshh foobar"));
    assertEquals(vr.getValidity(), Validity.INVALID);

    vr = v.validate(ConfigFactory.parseString("query = REFROOSH db.foobar"));
    assertEquals(vr.getValidity(), Validity.INVALID);
  }

}
