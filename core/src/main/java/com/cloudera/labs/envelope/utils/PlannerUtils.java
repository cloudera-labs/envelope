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

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.time.TimeModel;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

public class PlannerUtils {

  /**
   * Return a copy of the provided row with the mutation type set.
   * If the mutation type field is not present it will be added.
   * This does not modify the provided row.
   */
  public static Row setMutationType(Row row, MutationType mutationType) {
    if (!hasMutationTypeField(row)) {
      row = appendMutationTypeField(row);
    }
    
    return RowUtils.set(row, MutationType.MUTATION_TYPE_FIELD_NAME, mutationType.toString());
  }
  
  /**
   * Get the mutation type of the provided row. Throws an exception
   * if the row does not contain a mutation type field.
   */
  public static MutationType getMutationType(Row row) {
    assertHasMutationTypeField(row);
    
    return MutationType.valueOf(row.<String>getAs(MutationType.MUTATION_TYPE_FIELD_NAME));
  }
  
  /**
   * Return a copy of the provided row with the mutation type field
   * appended and set to mutation type NONE. If the mutation type field
   * is already present the provided row is returned back unchanged.
   */
  public static Row appendMutationTypeField(Row row) {
    if (!hasMutationTypeField(row)) {
      row = RowUtils.append(row, MutationType.MUTATION_TYPE_FIELD_NAME, DataTypes.StringType, MutationType.NONE.toString());
    }
    
    return row;
  }
  
  /**
   * Return a copy of the provided row with the mutation type field
   * removed. Throws an exception if the row does not contain a mutation
   * type field.
   */
  public static Row removeMutationTypeField(Row row) {
    assertHasMutationTypeField(row);
    
    return RowUtils.remove(row, MutationType.MUTATION_TYPE_FIELD_NAME);
  }
  
  /**
   * True if the provided row contains a mutation type field.
   */
  public static boolean hasMutationTypeField(Row row) {
    for (String fieldName : row.schema().fieldNames()) {
      if (fieldName.equals(MutationType.MUTATION_TYPE_FIELD_NAME)) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Returns a copy of intoRow where its time (as defined by intoRowModel)
   * is overwritten with the time from fromRow (as defined by fromTimeModel).
   * The two time models must be of the same type, but can be different
   * instances. This method does not modify fromRow or intoRow. 
   */
  public static Row copyTime(Row fromRow, TimeModel fromTimeModel, Row into, TimeModel intoTimeModel) {
    assertCompatibleTimeModels(fromTimeModel, intoTimeModel);
    
    Row fromTime = fromTimeModel.getTime(fromRow);
    
    for (int fieldNum = 0; fieldNum < fromTimeModel.getSchema().size(); fieldNum++) {
      into = RowUtils.set(into, intoTimeModel.getSchema().fieldNames()[fieldNum], fromTime.get(fieldNum));
    }
    
    return into;
  }
  
  /**
   * Returns a copy of intoRow where its time (as defined by intoRowModel)
   * is overwritten with the preceding time from fromRow (as defined by
   * fromTimeModel). The two time models must be of the same type, but can
   * be different instances. This method does not modify fromRow or intoRow. 
   */
  public static Row copyPrecedingTime(Row fromRow, TimeModel fromTimeModel, Row into, TimeModel intoTimeModel) {
    assertCompatibleTimeModels(fromTimeModel, intoTimeModel);
    
    Row fromTime = fromTimeModel.getPrecedingTime(fromRow);
    
    for (int fieldNum = 0; fieldNum < fromTimeModel.getSchema().size(); fieldNum++) {
      into = RowUtils.set(into, intoTimeModel.getSchema().fieldNames()[fieldNum], fromTime.get(fieldNum));
    }
    
    return into;
  }
  
  /**
   * If the first row is earlier in time than the second row by
   * referring to the fields of the time model instance.
   */
  public static boolean before(TimeModel timeModel, Row first, Row second) {
    return timeModel.compare(first, second) < 0;
  }
  
  /**
   * If the first row is at the same time as the second row by
   * referring to the fields of the time model instance.
   */
  public static boolean simultaneous(TimeModel timeModel, Row first, Row second) {
    return timeModel.compare(first, second) == 0;
  }
  
  /**
   * If the first row is later in time than the second row by
   * referring to the fields of the time model instance.
   */
  public static boolean after(TimeModel timeModel, Row first, Row second) {
    return timeModel.compare(first, second) > 0;
  }

  private static void assertHasMutationTypeField(Row row) {
    if (!hasMutationTypeField(row)) {
      throw new RuntimeException("No mutation type field found: " + row);
    }
  }
  
  private static void assertCompatibleTimeModels(TimeModel firstTimeModel, TimeModel secondTimeModel) {
    if (!firstTimeModel.getClass().equals(secondTimeModel.getClass())) {
      throw new RuntimeException(String.format("Incompatible time models between fields %s and %s", firstTimeModel, secondTimeModel));
    }
  }

}
