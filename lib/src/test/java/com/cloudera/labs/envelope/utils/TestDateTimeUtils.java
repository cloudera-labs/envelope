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

package com.cloudera.labs.envelope.utils;

import com.clearspring.analytics.util.Lists;
import com.cloudera.labs.envelope.utils.DateTimeUtils.DateTimeParser;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

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
    assertEquals(new LocalDateTime(parser.parse("2018-09-19 23:49:29.92284")).
        toDateTime(DateTimeZone.UTC).toString(), "2018-09-19T23:49:29.922Z");
  }

  @Test
  public void testParserFormats() {
    DateTimeParser parser = new DateTimeParser();
    parser.configureFormat(Lists.newArrayList(
        Arrays.asList("yyyy-MM-dd HH:mm:ss.SSSSS",
            "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss",
            "dd")));
    assertEquals(new LocalDateTime(parser.parse("2018-09-19 23:49:29.92284")).
        toDateTime(DateTimeZone.UTC).toString(), "2018-09-19T23:49:29.922Z");
    assertEquals(new LocalDateTime(parser.parse("2018-09-19 23:49:29")).
        toDateTime(DateTimeZone.UTC).toString(), "2018-09-19T23:49:29.000Z");
    assertEquals(new LocalDateTime(parser.parse("2018-07-25")).
        toDateTime(DateTimeZone.UTC).toString(), "2018-07-25T00:00:00.000Z");
    assertEquals(new LocalDateTime(parser.parse("08")).
        toDateTime(DateTimeZone.UTC).toString(), "2000-01-08T00:00:00.000Z");
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
