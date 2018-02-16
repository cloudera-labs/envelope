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
package com.cloudera.labs.envelope.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.output.RandomOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.PlannerUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

public class ZooKeeperOutput implements RandomOutput, Watcher, ProvidesAlias {

  public static final String CONNECTION_CONFIG = "connection";
  public static final String FIELD_NAMES_CONFIG = "field.names";
  public static final String FIELD_TYPES_CONFIG = "field.types";
  public static final String KEY_FIELD_NAMES_CONFIG = "key.field.names";
  public static final String ZNODE_PREFIX_CONFIG = "znode.prefix";
  public static final String SESSION_TIMEOUT_MS_CONFIG = "session.timeout.millis";
  
  private static final String DEFAULT_ZNODE_PREFIX = "/envelope";
  private static final int DEFAULT_SESSION_TIMEOUT_MS = 1000;
  
  private String connection;
  private List<String> fieldNames;
  private List<String> fieldTypes;
  private List<String> keyFieldNames;
  private String znodePrefix;
  private int sessionTimeoutMs;

  private static ZooKeeper _zk;
  private static CountDownLatch latch;
  
  @Override
  public void configure(Config config) {
    ConfigUtils.assertConfig(config, CONNECTION_CONFIG);
    this.connection = config.getString(CONNECTION_CONFIG);
    
    ConfigUtils.assertConfig(config, KEY_FIELD_NAMES_CONFIG);
    this.keyFieldNames = config.getStringList(KEY_FIELD_NAMES_CONFIG);
    
    ConfigUtils.assertConfig(config, FIELD_NAMES_CONFIG);
    this.fieldNames = config.getStringList(FIELD_NAMES_CONFIG);
    
    ConfigUtils.assertConfig(config, FIELD_TYPES_CONFIG);
    this.fieldTypes = config.getStringList(FIELD_TYPES_CONFIG);
    
    if (config.hasPath(ZNODE_PREFIX_CONFIG)) {
      this.znodePrefix = config.getString(ZNODE_PREFIX_CONFIG);
    }
    else {
      this.znodePrefix = DEFAULT_ZNODE_PREFIX;
    }
    
    if (config.hasPath(SESSION_TIMEOUT_MS_CONFIG)) {
      this.sessionTimeoutMs = config.getInt(SESSION_TIMEOUT_MS_CONFIG);
    }
    else {
      this.sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;
    }
  }

  @Override
  public Set<MutationType> getSupportedRandomMutationTypes() {
    return Sets.newHashSet(MutationType.UPSERT, MutationType.DELETE);
  }

  @Override
  public void applyRandomMutations(List<Row> planned) throws Exception {
    if (planned.size() > 1000) {
      throw new RuntimeException(
          "ZooKeeper output does not support applying more than 1000 mutations at a time. " +
          "This is to prevent misuse of ZooKeeper as a regular data store. " + 
          "Do not use ZooKeeper for storing anything more than small pieces of metadata.");
    }
    
    ZooKeeper zk = getZooKeeper();
    
    for (Row plan : planned) {
      if (plan.schema() == null) {
        throw new RuntimeException("Mutation row provided to ZooKeeper output must contain a schema");
      }
      
      MutationType mutationType = PlannerUtils.getMutationType(plan);
      plan = PlannerUtils.removeMutationTypeField(plan);
      
      Row key = RowUtils.subsetRow(plan, RowUtils.subsetSchema(plan.schema(), keyFieldNames));
      String znode = znodesForFilter(zk, key).iterator().next(); // There can only be one znode per full key
      byte[] value = serializeRow(RowUtils.subsetRow(plan, RowUtils.subtractSchema(plan.schema(), keyFieldNames)));
      
      switch (mutationType) {
        case DELETE:
          zk.delete(znode, -1);
          break;
        case UPSERT:
          prepareZnode(zk, znode);
          zk.setData(znode, value, -1);
          break;
        default:
          throw new RuntimeException("ZooKeeper output does not support mutation type: " + PlannerUtils.getMutationType(plan));
      }
    }
  }

  @Override
  public Iterable<Row> getExistingForFilters(Iterable<Row> filters) throws Exception {
    ZooKeeper zk = getZooKeeper();
    
    Set<Row> existing = Sets.newHashSet();
    
    for (Row filter : filters) {
      List<String> znodes = znodesForFilter(zk, filter);
      
      for (String znode : znodes) {
        if (zk.exists(znode, false) != null) {
          byte[] serialized = zk.getData(znode, false, null);
          
          if (serialized.length > 0) {
            Row existingRow = toFullRow(znode, serialized);
            
            if (matchesValueFilter(existingRow, filter)) {
              existing.add(existingRow);
            }
          }
        }
      }
    }
    
    return existing;
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
      latch.countDown();
    }
  }
  
  private synchronized ZooKeeper getZooKeeper() {
    if (_zk == null || _zk.getState() != States.CONNECTED) {
      try {
        latch = new CountDownLatch(1);
        _zk = new ZooKeeper(this.connection, this.sessionTimeoutMs, this);
        latch.await();
      } catch (Exception e) {
        throw new RuntimeException("Could not connect to ZooKeeper output: " + e.getMessage());
      }
    }
    
    return _zk;
  }
  
  private void prepareZnode(ZooKeeper zk, String znode) throws KeeperException, InterruptedException {
    String[] znodeParts = znode.split(Pattern.quote("/"));
    
    String znodePrefix = "";
    for (String znodePart : znodeParts) {
      if (znodePart.length() > 0) {
        znodePrefix += "/" + znodePart;
        
        if (zk.exists(znodePrefix, false) == null) {
          zk.create(znodePrefix, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
      }
    }
  }
  
  private List<String> znodesForFilter(ZooKeeper zk, Row filter) throws KeeperException, InterruptedException {
    if (filter.schema() == null) {
      throw new RuntimeException("Existing filter provided to ZooKeeper output must contain a schema");
    }
    
    List<String> filterFieldNames = Lists.newArrayList(filter.schema().fieldNames());
    List<String> currentPaths = Lists.newArrayList(this.znodePrefix);
    
    prepareZnode(zk, this.znodePrefix);
    
    for (String keyFieldName : keyFieldNames) {
      List<String> nextPaths = Lists.newArrayList();
      
      for (String currentPath : currentPaths) {
        if (filterFieldNames.contains(keyFieldName)) {
          String nextPath = currentPath + "/" + keyFieldName + "=" + filter.get(filter.fieldIndex(keyFieldName));
          nextPaths.add(nextPath);
        }
        else {
          if (zk.exists(currentPath, false) != null) {
            List<String> children = zk.getChildren(currentPath, false);
            for (String child : children) {
              String nextPath = currentPath + "/" + child;
              nextPaths.add(nextPath);
            }
          }
        }
      }
      
      currentPaths = nextPaths;
    }
    
    return currentPaths;
  }
  
  private byte[] serializeRow(Row row) throws IOException {
    StringBuilder sb = new StringBuilder();
    
    for (StructField field : row.schema().fields()) {
      sb.append("/");
      sb.append(field.name());
      sb.append("=");
      sb.append(RowUtils.get(row, field.name()));
    }

    byte[] serialized = sb.toString().getBytes(Charsets.UTF_8);
    
    return serialized;
  }
  
  private Row toFullRow(String znode, byte[] serialized) throws ClassNotFoundException, IOException {
    StructType schema = RowUtils.structTypeFor(fieldNames, fieldTypes);
    
    String values = new String(serialized, Charsets.UTF_8);
    String fullPath = znode + values;
    String[] levels = fullPath.replace(this.znodePrefix,  "").split(Pattern.quote("/"));
    List<Object> objects = Lists.newArrayList();
    
    for (String level : levels) {
      if (level.length() > 0) {
        String[] znodeLevelParts = level.split(Pattern.quote("="));
        String fieldName = znodeLevelParts[0];
        String fieldValueString = znodeLevelParts[1];
        String fieldType = fieldTypes.get(fieldNames.indexOf(fieldName));
        Object value;
        
        switch (fieldType) {
          case "string":
            value = fieldValueString;
            break;
          case "float":
            value = Float.parseFloat(fieldValueString);
            break;
          case "double":
            value = Double.parseDouble(fieldValueString);
            break;
          case "int":
            value = Integer.parseInt(fieldValueString);
            break;
          case "long":
            value = Long.parseLong(fieldValueString);
            break;
          case "boolean":
            value = Boolean.parseBoolean(fieldValueString);
            break;
          default:
            throw new RuntimeException("ZooKeeper output does not support data type: " + fieldType);
        }
        
        objects.add(value);
      }
    }
    
    Row fullRow = new RowWithSchema(schema, objects.toArray());
    
    return fullRow;
  }
  
  private boolean matchesValueFilter(Row row, Row filter) {
    for (String filterFieldName : filter.schema().fieldNames()) {
      Object rowValue = row.get(row.fieldIndex(filterFieldName));
      Object filterValue = RowUtils.get(filter, filterFieldName);
      
      if (!rowValue.equals(filterValue)) {
        return false;
      }
    }
    
    return true;
  }

  @Override
  public String getAlias() {
    return "zookeeper";
  }
}
