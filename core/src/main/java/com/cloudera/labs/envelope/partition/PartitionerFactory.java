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

package com.cloudera.labs.envelope.partition;

import com.cloudera.labs.envelope.load.LoadableFactory;
import com.typesafe.config.Config;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import scala.math.Ordering;
import scala.math.Ordering$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.Comparator;

public class PartitionerFactory extends LoadableFactory<ConfigurablePartitioner> {

  public static final String TYPE_CONFIG_NAME = "type";

  public static Partitioner create(Config config, JavaPairRDD<Row, Row> rdd) {
    String partitionerType = config.getString(TYPE_CONFIG_NAME);
    
    if (!config.hasPath(TYPE_CONFIG_NAME)) {
      throw new RuntimeException("Partitioner type not specified");
    }

    Partitioner partitioner;

    switch (partitionerType) {
      case "hash":
        partitioner = new HashPartitioner(rdd.getNumPartitions());
        break;
      case "range":
        Ordering<Row> rowOrdering = Ordering$.MODULE$.<Row>comparatorToOrdering(new RowComparator());
        ClassTag<Row> rowClassTag = ClassTag$.MODULE$.<Row>apply(Row.class);
        partitioner = new RangePartitioner<Row, Row>(rdd.getNumPartitions(), rdd.rdd(), true, rowOrdering, rowClassTag);
        break;
      default:
        try {
          partitioner = loadImplementation(ConfigurablePartitioner.class, partitionerType);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
    }
    
    if (partitioner == null) {
      throw new RuntimeException("No partitioner implementation found for: " + partitionerType);
    }

    if (partitioner instanceof ConfigurablePartitioner) {
      ((ConfigurablePartitioner)partitioner).configure(config, rdd);
    }

    return partitioner;
  }
  
  @SuppressWarnings("serial")
  private static class RowComparator implements Comparator<Row>, Serializable {
    // All primitive value types of Row implement Comparable
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public int compare(Row r1, Row r2) {
      for (int i = 0; i < r1.length(); i++) {
        Comparable r1FieldValue = (Comparable)r1.get(i);
        Comparable r2FieldValue = (Comparable)r2.get(i);
        
        int comparison = r1FieldValue.compareTo(r2FieldValue);
        
        if (comparison != 0) {
          return comparison;
        }
      }
      
      return 0;
    }
  }
  
}
