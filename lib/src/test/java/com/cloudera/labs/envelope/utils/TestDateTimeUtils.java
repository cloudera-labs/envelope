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
package com.cloudera.labs.envelope.utils;

import static org.junit.Assert.assertEquals;

import com.clearspring.analytics.util.Lists;
import com.cloudera.labs.envelope.utils.DateTimeUtils.DateTimeParser;
import org.junit.Test;

import java.util.Arrays;

public class TestDateTimeUtils {

  @Test
  public void testParserDefaultISOFormat() {
    DateTimeParser parser = new DateTimeParser();
    assertEquals(parser.parse("2018-03-03T20:23:33.897+04:00").getMillis(),
        1520094213897L);
  }

  @Test
  public void testParserOneFormat() {
    DateTimeParser parser = new DateTimeParser();
    parser.configureFormat(Lists.newArrayList(
        Arrays.asList("yyyy-MM-dd HH:mm:ss.SSSSS")));
    assertEquals(parser.parse("2018-09-19 23:49:29.92284").getMillis(),
        1537415369922L);
  }

  @Test
  public void testParserFormats() {
    DateTimeParser parser = new DateTimeParser();
    parser.configureFormat(Lists.newArrayList(
        Arrays.asList("yyyy-MM-dd HH:mm:ss.SSSSS",
            "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss",
            "dd")));
    assertEquals(parser.parse("2018-09-19 23:49:29.92284").getMillis(),
        1537415369922L);
    assertEquals(parser.parse("2018-09-19 23:49:29").getMillis(),
        1537415369000L);
    assertEquals(parser.parse("2018-07-25").getMillis(),
        1532491200000L);
    assertEquals(parser.parse("08").getMillis(),
        947307600000L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParserInvalidInput() {
    DateTimeParser parser = new DateTimeParser();
    parser.configureFormat(Lists.newArrayList(
        Arrays.asList("yyyy-MM-dd HH:mm:ss.SSSSS",
            "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss",
            "dd")));
    parser.parse("09-19 23:29.92284");
  }
}
