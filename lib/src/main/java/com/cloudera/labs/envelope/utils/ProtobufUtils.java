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

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

/**
 * Utilities for proto3
 * <p>
 * This does not support the following field types: {@code Any}.
 * This does not support any extensions.
 * This also ignores any declared services.
 */
public class ProtobufUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ProtobufUtils.class);
  private static final String KEY_TYPE = "key";
  private static final String VALUE_TYPE = "value";

  /**
   * Retrieves and converts Protobuf fields from a Message.
   * <p>
   * If the field in the {@link com.google.protobuf.Descriptors.Descriptor} exists in the {@link Message}, the value is
   * retrieved and converted using {@link #getFieldValue(Descriptors.FieldDescriptor, Object, DataType)}.
   * Otherwise, the field value is {@code null}.
   * The extraction honors the order of the {@code Descriptor}.
   *
   * @param dsc the Protobuf Descriptor with all fields
   * @param msg the Message with the current field values
   * @param schema the Dataset schema derived from the Descriptor
   * @return a list of converted values
   */
  public static List<Object> buildRowValues(Descriptors.Descriptor dsc, Message msg, StructType schema) {
    List<Object> values = new ArrayList<>();
    Object val;

    for (Descriptors.FieldDescriptor fd : dsc.getFields()) {
      if ( (!fd.isRepeated() && msg.hasField(fd)) || (fd.isRepeated() && msg.getRepeatedFieldCount(fd) > 0) ) {
        val = getFieldValue(fd, msg.getField(fd), schema.apply(fd.getName()).dataType());
      } else {
        LOG.trace("FieldDescriptor[{}] => not found", fd.getFullName());
        val = null;
      }
      values.add(val);
    }

    return values;
  }

  /**
   * Convert a {@code Message} field value according to the {@code FieldDescriptor}.
   * <p>
   * This will recurse into nested and repeated fields and unwrap the values into {@code Row}-compatible values.
   *
   * @param field the FieldDescriptor within the Message
   * @param value the value to convert
   * @param dataType the DataType associated with output schema
   * @return the Row-compatible value
   */
  static Object getFieldValue(Descriptors.FieldDescriptor field, Object value, DataType dataType) {
    // From DynamicMessage.getField():
    // Obtains the value of the given field, or the default value if it is not set.
    // For primitive fields, the boxed primitive value is returned.
    // For enum fields, the EnumValueDescriptor for the value is returned.
    // For embedded message fields, the sub-message is returned.
    // For repeated fields, a java.util.List is returned.

    Object val;

    // If field is null, return
    if (value == null) {
      val = null;
    } else {

      // Map
      if (field.isMapField()) {
        // A list of messages, each with two fields, 'key' and 'value'
        LOG.trace("FieldDescriptor[{}] is a Map, unrolling", field.getFullName());

        MapType mapType = (MapType) dataType;

        @SuppressWarnings("unchecked")
        List<DynamicMessage> mapList = (List<DynamicMessage>) value;
        HashMap<Object, Object> convertedMap = new HashMap<>();

        Descriptors.FieldDescriptor keyField = field.getMessageType().findFieldByName(KEY_TYPE);
        Descriptors.FieldDescriptor valueField = field.getMessageType().findFieldByName(VALUE_TYPE);

        for (DynamicMessage mapEntry : mapList) {
          Object k = RowUtils.toRowValue(mapEntry.getField(keyField), mapType.keyType());
          Object v = getFieldValue(valueField, mapEntry.getField(valueField), mapType.valueType());
          convertedMap.put(k, v);
        }

        val = JavaConversions.mapAsScalaMap(convertedMap);
      }

      // List
      else if (field.isRepeated()) {
        // A list of either messages, scalars, or enums
        LOG.trace("FieldDescriptor[{}] is List, unrolling", field.getFullName());

        ArrayType arrayType = (ArrayType) dataType;

        @SuppressWarnings("unchecked")
        List<Object> children = (List<Object>) value;
        List<Object> childValues = new ArrayList<>();

        for (Object child : children) {
          switch (field.getType()) {
            case MESSAGE:
              Row childRow = RowFactory.create(buildRowValues(field.getMessageType(), (Message) child,
                  (StructType) arrayType.elementType()).toArray());
              childValues.add(childRow);
              break;
            case ENUM:
              childValues.add(RowUtils.toRowValue(((Descriptors.EnumValueDescriptor) child).getName(),
                  arrayType.elementType()));
              break;
            case GROUP:
              throw new IllegalStateException("GROUP type not permitted");
            default:
              childValues.add(RowUtils.toRowValue(child, arrayType.elementType()));
          }
        }

        val = JavaConverters.asScalaBufferConverter(childValues).asScala().toSeq();
      }

      // Embedded Message
      else if (field.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE)) {
        LOG.trace("FieldDescriptor[{}] is a Message, unrolling", field.getFullName());
        val = RowFactory.create(buildRowValues(field.getMessageType(), (DynamicMessage) value,
            (StructType) dataType).toArray());
      }

      // Enum
      else if (field.getType().equals(Descriptors.FieldDescriptor.Type.ENUM)) {
        val = RowUtils.toRowValue(((Descriptors.EnumValueDescriptor) value).getName(), dataType);
      }

      // Bytes
      else if (field.getType().equals(Descriptors.FieldDescriptor.Type.BYTES)) {
        val = RowUtils.toRowValue(((ByteString) value).toByteArray(), dataType);
      }

      // Primitive
      else {
        val = RowUtils.toRowValue(value, dataType);
      }
    }

    LOG.trace("FieldDescriptor[{}] => Value[{}]", field.getFullName(), val);
    return val;
  }

  /**
   * Construct a {@code Dataset} schema from a {@code Descriptor}
   * <p>
   * This iterates and recurses through a {@link com.google.protobuf.Descriptors.Descriptor} and produces a
   * {@link StructType} for {@link org.apache.spark.sql.Dataset<Row>}.
   * Protobuf {@code oneof} fields are flattened into discrete {@link StructField} instances.
   * <p>
   * This will pass the value of {@link Descriptors.FieldDescriptor#isRequired()} to the associated {@link StructField}.
   *
   * @param descriptor the Descriptor to convert
   * @return the converted StructType
   */
  public static StructType buildSchema(Descriptors.Descriptor descriptor) {
    List<StructField> members = new ArrayList<>();
    List<Descriptors.FieldDescriptor> protoFields = descriptor.getFields();

    for (Descriptors.FieldDescriptor fieldDescriptor : protoFields) {
      DataType fieldType = convertType(fieldDescriptor);
       StructField structField = DataTypes.createStructField(fieldDescriptor.getName(), fieldType,
          !fieldDescriptor.isRequired());
      members.add(structField);
      LOG.debug("FieldDescriptor[{}] => StructField[{}] ", fieldDescriptor.getFullName(), structField);
    }

    if (members.isEmpty()) {
      throw new RuntimeException("No FieldDescriptors found");
    }

    return DataTypes.createStructType(members.toArray(new StructField[0]));
  }

  /**
   * Constructs a named {@code Message} from a {@code .desc} file into a {@code Descriptor}.
   * <p>
   * This will parse the {@code .desc} file and then extract the encapsulated
   * {@link com.google.protobuf.Descriptors.Descriptor} the designated {@link Message} from the resulting parent
   * {@link Descriptors.FileDescriptor}.
   *
   * @param filePath the location of the .desc file
   * @param messageName the name of the Message
   * @return the constructed Descriptor
   */
  public static Descriptors.Descriptor buildDescriptor(String filePath, String messageName) {
    Descriptors.FileDescriptor fileDescriptor = parseFileDescriptor(filePath);
    Descriptors.Descriptor result = null;

    for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {
      if (descriptor.getName().equals(messageName)) {
        result = descriptor;
      }
    }

    if (null == result) {
      throw new RuntimeException("Unable to locate Message '" + messageName + "' in Descriptor");
    }

    return result;
  }

  /**
   * Constructs the default {@code Message} from a {@code .proto} file into a {@code Descriptor}.
   *
   * @param filePath the location of the .desc file
   * @return the constructed Descriptor
   */
  public static Descriptors.Descriptor buildDescriptor(String filePath) {
    Descriptors.FileDescriptor fileDescriptor = parseFileDescriptor(filePath);
    if (fileDescriptor.getMessageTypes().size() > 1) {
      throw new RuntimeException("Multiple top-level MessageTypes found with no designated Message");
    }
    return fileDescriptor.getMessageTypes().get(0);
  }

  /**
   * Tests if an {@code InputStream} is gzipped.
   *
   * @param inputStream the InputSteam to test
   * @return true is the stream is compressed
   * @see GZIPInputStream
   */
  public static boolean isGzipped(InputStream inputStream) {
    if (!inputStream.markSupported()) {
      inputStream = new BufferedInputStream(inputStream);
    }
    inputStream.mark(2);
    int magic = 0;
    try {
      magic = inputStream.read() & 0xFF | ((inputStream.read() << 8) & 0xFF00);
      inputStream.reset();
    } catch (IOException e) {
      throw new RuntimeException("Unable to read InputStream", e);
    }
    return magic == GZIPInputStream.GZIP_MAGIC;
  }

  private static Descriptors.FileDescriptor parseFileDescriptor(String filePath) {
    LOG.debug("Reading Descriptor file: " + filePath);
    DescriptorProtos.FileDescriptorSet fileDescriptorSet;

    try (BufferedInputStream dscFile = new BufferedInputStream(new FileInputStream(filePath))) {
      fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(dscFile);
    } catch (InvalidProtocolBufferException ex) {
      throw new RuntimeException("Error parsing descriptor byte[] into Descriptor object", ex);
    } catch (IOException ex) {
      throw new RuntimeException("Error reading file at path: " + filePath, ex);
    }

    // Get the attached .proto file
    DescriptorProtos.FileDescriptorProto descriptorProto = fileDescriptorSet.getFile(0);

    try {
      Descriptors.FileDescriptor fileDescriptor =  Descriptors.FileDescriptor.buildFrom(descriptorProto,
          new Descriptors.FileDescriptor[]{});

      // Check to see if anything was returned
      if (fileDescriptor.getMessageTypes().isEmpty()) {
        throw new RuntimeException("No MessageTypes returned, " + fileDescriptor.getName());
      }

      return fileDescriptor;
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException("Error constructing FileDescriptor", e);
    }
  }

  /**
   * Map a {@code FieldDescriptor} to a {@code DataType}.
   * <p>
   * This will match {@link com.google.protobuf.Descriptors.FieldDescriptor} types to {@link DataType} according to the
   * following table:
   * <p>
   * <table>
   *   <th>
   *     <td>FieldDescriptor</td>
   *     <td>DataType</td>
   *   </th>
   *   <tr>
   *     <td>BOOLEAN</td>
   *     <td>Boolean</td>
   *   </tr>
   *   <tr>
   *     <td>BYTE_STRING</td>
   *     <td>Binary</td>
   *   </tr>
   *   <tr>
   *     <td>DOUBLE</td>
   *     <td>Double</td>
   *   </tr>
   *   <tr>
   *     <td>ENUM</td>
   *     <td>String</td>
   *   </tr>
   *   <tr>
   *     <td>FLOAT</td>
   *     <td>Float</td>
   *   </tr>
   *   <tr>
   *     <td>INT</td>
   *     <td>Integer</td>
   *   </tr>
   *   <tr>
   *     <td>LONG</td>
   *     <td>Long</td>
   *   </tr>
   *   <tr>
   *     <td>MESSAGE</td>
   *     <td>StructType</td>
   *   </tr>
   *   <tr>
   *     <td>STRING</td>
   *     <td>String</td>
   *   </tr>
   * </table>
   * <p>
   * If the field is a MESSAGE, this will check to see if the field is a {@code Map} construct, and if so will recurse
   * into each key/value pair and create a {@link org.apache.spark.sql.types.MapType}, otherwise will recurse into the
   * field and construct the {@link StructType}.
   * <p>
   * If the field is repeating and not a {@code Map} construct, this will wrap the conversion into an
   * {@link org.apache.spark.sql.types.ArrayType}.
   *
   * @param field the FieldDescriptor to convert
   * @return the resulting DataType
   */
  static DataType convertType(Descriptors.FieldDescriptor field) {
    Descriptors.FieldDescriptor.JavaType type = field.getJavaType();
    DataType dataType;

    switch (type) {
      case BOOLEAN:
        dataType = DataTypes.BooleanType;
        break;
      case BYTE_STRING:
        dataType = DataTypes.BinaryType;
        break;
      case DOUBLE:
        dataType = DataTypes.DoubleType;
        break;
      case ENUM:
        dataType = DataTypes.StringType;
        break;
      case FLOAT:
        dataType = DataTypes.FloatType;
        break;
      case INT:
        dataType = DataTypes.IntegerType;
        break;
      case LONG:
        dataType = DataTypes.LongType;
        break;
      case MESSAGE:
        Descriptors.Descriptor msg = field.getMessageType();
        // Recurse as needed
        if (field.isMapField()) {
          dataType = DataTypes.createMapType(convertType(msg.findFieldByName(KEY_TYPE)), convertType(
              msg.findFieldByName(VALUE_TYPE)), true);
        } else {
          dataType = buildSchema(field.getMessageType());
        }
        break;
      case STRING:
        dataType = DataTypes.StringType;
        break;
      default:
        throw new RuntimeException("Unknown type: " + type.toString() + " for FieldDescriptor: " + field.toString());
    }

    if (field.isRepeated() && !field.isMapField()) {
      dataType = DataTypes.createArrayType(dataType, true);
    }

    LOG.trace("FieldDescriptor[{}] => {}", field.getFullName(), dataType);
    return dataType;
  }
}
