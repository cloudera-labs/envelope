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

package com.cloudera.labs.envelope.event;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

public class EventUtils {

  public static String prettifyNs(long ns) {
    BigDecimal bd = new BigDecimal(ns);
    String uom = "ns";

    while (bd.subtract(BigDecimal.valueOf(999)).compareTo(BigDecimal.ONE) >= 0 && !uom.equals("s")) {
      bd = bd.divide(new BigDecimal(1000), new MathContext(20, RoundingMode.FLOOR));
      switch (uom) {
        case "ns":
          uom = "us";
          break;
        case "us":
          uom = "ms";
          break;
        case "ms":
          uom = "s";
          break;
      }
    }

    bd = bd.setScale(2, RoundingMode.FLOOR);
    bd = bd.stripTrailingZeros();

    return bd.toPlainString() + uom;
  }

  public static String getCallingClassName() {
    StackTraceElement[] stes = Thread.currentThread().getStackTrace();

    for (int i = 2; i < stes.length; i++) {
      if (!stes[i].getClassName().startsWith(Event.class.getPackage().getName()) ||
           stes[i].getClassName().startsWith(Event.class.getPackage().getName() + ".Test")) {
        return stes[i].getClassName();
      }
    }

    throw new RuntimeException("Could not determine class that provided event");
  }

}
