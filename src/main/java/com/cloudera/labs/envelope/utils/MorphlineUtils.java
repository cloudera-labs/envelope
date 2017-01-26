package com.cloudera.labs.envelope.utils;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.kitesdk.morphline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MorphlineUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MorphlineUtils.class);

  /**
   * <p>Converts a Morphline {@link Record} into a Spark DataFrame {@link Row}.  The first value of Record's field is
   * used; all other values for the given field are ignored.</p>
   * <p>Throws a RuntimeException on conversion error.</p>
   * @see Record#getFirstValue(String)
   * @param schema The target Row schema
   * @param record The source Record object
   * @return A Row object based on the target schema
   */
  public static Row convertToRow(StructType schema, Record record) {
    LOG.debug("Converting Record to Row: {}", record);

    List<Object> values = Lists.newArrayList();
    ListMultimap activeFields = record.getFields();

    for (StructField field : schema.fields()) {
      String fieldName = field.name();
      DataType fieldDataType = field.dataType();

      if (activeFields.containsKey(fieldName)) {
        Object recordValue = record.getFirstValue(fieldName);

        if (LOG.isTraceEnabled()) {
          LOG.trace("Converting Field[{} => {}] to DataType[{}]]", fieldName, recordValue, fieldDataType);
        }

        if (null != recordValue) {
          try {
            Object result = RowUtils.toRowValue(recordValue, field.dataType());
            values.add(result);
          } catch (Exception e) {
            throw new RuntimeException(String.format("Error converting Field[%s => %s] to DataType[%s]", fieldName,
                recordValue, fieldDataType), e);
          }
        } else {
          if (field.nullable()) {
            LOG.trace("Setting Field[{} => null] for DataType[{}]", fieldName, fieldDataType);
            values.add(null);
          } else {
            throw new RuntimeException(String.format("Error converting Field[%s => null] for DataType[%s]: DataType " +
                "cannot contain 'null'", fieldName, fieldDataType));
          }
        }
      } else {
        throw new RuntimeException(String.format("Error converting Record: missing Field[%s]'", fieldName));
      }
    }

    Row result = RowFactory.create(values.toArray());
    LOG.trace("Converted Record to Row: {}", result);

    return result;
  }
}
