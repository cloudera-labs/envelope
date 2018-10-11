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

package com.cloudera.labs.envelope.kafka;

import com.cloudera.labs.envelope.output.RandomOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
   Dummy RandomOutput that stores Kafka offsets as Spark SQL Rows.
   Assumes:
   - Input Row schema of {group_id, topic, partition, offset}
   - Key is first three fields of the input Row .
   - Lookup is by {group_id, topic}.
   - Only Upserts (no Deletes).
 */
public class DummyKafkaOffsetStore implements RandomOutput {
  public Map<String, Row> store = Maps.newHashMap();

  @Override
  public Set<MutationType> getSupportedRandomMutationTypes() {
    return Sets.newHashSet(MutationType.UPSERT);
  }

  @Override
  public void applyRandomMutations(List<Row> planned) throws Exception {
    for (Row row : planned) {
      String groupID = row.getString(0);
      String topic = row.getString(1);
      int partition = row.getInt(2);
      String key = groupID+topic+partition;
      store.put(key, row);
    }
  }

  @Override
  public Iterable<Row> getExistingForFilters(Iterable<Row> filters) throws Exception {
    List<Row> matches = Lists.newArrayList();
    for (Row filter : filters) {
      List<String> filterFieldNames = Lists.newArrayList(filter.schema().fieldNames());
      assert (filterFieldNames.get(0).equals("group_id"));
      assert (filterFieldNames.get(1).equals("topic"));
      String groupID = filter.getString(0);
      String topic = filter.getString(1);
      for (Row row : store.values()) {
        if (row.getString(0).equals(groupID) &&
            row.getString(1).equals(topic)) {
          matches.add(row);
        }
      }
    }
    return matches;
  }

  @Override
  public void configure(Config config) {
  }
}
