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

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;

public class DateTimeUtils {

  /**
   * A utility class for parsing timestamps
   * using an <em>optional set</em>of datetime patterns.
   * When patterns are supplied, at least one of the patterns
   * must be applicable for the given input for the input to
   * be parsed.
   * <p>
   * When patterns are not supplied, uses generic ISO datetime parser
   * as described in {@link org.joda.time.format.ISODateTimeFormat}
   * <p>
   * When patterns are supplied, for performance sensitive processing,
   * list patterns in order of probability of occurrence.
   * To supply patterns, use Joda pattern syntax as described here:
   * @see <a href="https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html">Joda DateTimeFormat</a>
   *
   * @throws IllegalArgumentException
   *
   * TODO: Migrate to JDK 8 Time API upon JDK 8 migration.
   */
  public static class DateTimeParser {
    private List<DateTimeFormatter> formatters =
        new ArrayList();

    public void configureFormat(List<String> patterns)
        throws IllegalArgumentException {
      if (patterns == null) {
        throw new IllegalArgumentException("Supplied patterns" +
            "argument cannot be null.");
      }

      for (String pattern : patterns) {
        formatters.add(DateTimeFormat.forPattern(pattern));
      }
    }

    public DateTime parse(String date) {
      if (formatters.size() == 0) {
        return DateTime.parse(date);
      }

      for (DateTimeFormatter dateTimeFormatter : formatters) {
        try {
          return dateTimeFormatter.parseDateTime(date);
        } catch (IllegalArgumentException e) {
          // ignore
        }
      }

      throw new IllegalArgumentException("Could not parse: "
          + date);
    }
  }
}