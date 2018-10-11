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

package com.cloudera.labs.envelope;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Envelope Main (entry point) JUnit Test
 */
public class TestEnvelopeMain {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testMainNoConfig() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Missing");
    String[] empty = {};
    EnvelopeMain.main(empty);
  }

  @Test
  public void testMainBadConfig() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Can't access");
    String[] conf = { "xxx.conf" };
    EnvelopeMain.main(conf);
  }

  @Test
  public void testMainGoodConfig() throws Exception {
    Path p = Files.createTempFile("TestEnvelopeMain", null ) ;
    Files.write(p,"application {}\nsteps{}".getBytes()) ;
    p.toFile().deleteOnExit();
    String[] conf = { p.toString() };
    EnvelopeMain.main(conf);
  }

}