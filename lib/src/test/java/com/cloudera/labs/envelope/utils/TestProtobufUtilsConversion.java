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

import com.cloudera.labs.envelope.input.translate.ProtobufSingleMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import scala.collection.Seq;

/**
 *
 */
@RunWith(Parameterized.class)
public class TestProtobufUtilsConversion {

  private static final String SINGLE_EXAMPLE = "/protobuf/protobuf_single_message.desc";
  private static final Descriptors.Descriptor SINGLE_DESC;

  static {
    DescriptorProtos.FileDescriptorSet fileDescriptorSet;

    try (BufferedInputStream dscFile = new BufferedInputStream(new FileInputStream(
        TestProtobufUtils.class.getResource(SINGLE_EXAMPLE).getPath()
    ))) {
      fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(dscFile);
    } catch (Throwable throwable) {
      throw new RuntimeException("Unable to construct test resources", throwable);
    }

    // Get the attached .proto file
    DescriptorProtos.FileDescriptorProto descriptorProto = fileDescriptorSet.getFile(0);

    try {
      Descriptors.FileDescriptor fileDescriptor =  Descriptors.FileDescriptor.buildFrom(descriptorProto,
          new Descriptors.FileDescriptor[]{});
      if (fileDescriptor.getMessageTypes().isEmpty()) {
        throw new RuntimeException("No MessageTypes returned, " + fileDescriptor.getName());
      }

      SINGLE_DESC = fileDescriptor.getMessageTypes().get(0);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException("Error constructing FileDescriptor", e);
    }
  }

  public static DynamicMessage message;

  @BeforeClass
  public static void createMessages() throws IOException {
    ProtobufSingleMessage.SingleExample msg = ProtobufSingleMessage.SingleExample.newBuilder()
        .setString("foo")
        .setDouble(123.456D)
        .setFloat(123.456F)
        .setInt32(123)
        .setInt64(123L)
        .setUint32(123)
        .setUint64(123L)
        .setSint32(123)
        .setSint64(123L)
        .setFixed32(123)
        .setFixed64(123L)
        .setSfixed32(123)
        .setSfixed64(123L)
        .setBoolean(true)
        .setBytes(ByteString.copyFromUtf8("foo"))
        .setEnum(ProtobufSingleMessage.SingleExample.EnumExample.THREE)
        .setNested(ProtobufSingleMessage.SingleExample.NestedExample.newBuilder()
          .setNested("single")
        )
        .putMapInt("first", 1)
        .putMapInt("second", 2)
        .addRepeatingMessage(ProtobufSingleMessage.SingleExample.NestedExample.newBuilder()
            .setNested("repeated 1")
        )
        .addRepeatingMessage(ProtobufSingleMessage.SingleExample.NestedExample.newBuilder()
            .setNested("repeated 2")
        )
        .addRepeatingInt32(567)
        .addRepeatingInt32(890)
        .addRepeatingEnum(ProtobufSingleMessage.SingleExample.EnumExample.ONE)
        .addRepeatingEnum(ProtobufSingleMessage.SingleExample.EnumExample.TWO)
        .build();

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      msg.writeTo(outputStream);
      message = DynamicMessage.parseFrom(SINGLE_DESC, outputStream.toByteArray());
    }
  }

  @Parameters(name = "{0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][]{

        { "string", DataTypes.StringType, String.class },
        { "double" , DataTypes.DoubleType, Double.class },
        { "float" , DataTypes.FloatType, Float.class },
        { "int32" , DataTypes.IntegerType, Integer.class },
        { "int64" , DataTypes.LongType, Long.class },
        { "uint32" , DataTypes.IntegerType, Integer.class },
        { "uint64" , DataTypes.LongType, Long.class },
        { "sint32" , DataTypes.IntegerType, Integer.class },
        { "sint64" , DataTypes.LongType, Long.class },
        { "fixed32" , DataTypes.IntegerType, Integer.class },
        { "fixed64" , DataTypes.LongType, Long.class },
        { "sfixed32" , DataTypes.IntegerType, Integer.class },
        { "sfixed64" , DataTypes.LongType, Long.class },
        { "boolean" , DataTypes.BooleanType, Boolean.class },
        { "bytes" , DataTypes.BinaryType, byte[].class },
        { "enum" , DataTypes.StringType, String.class  },
        {
          "nested",
            DataTypes.createStructType(
                Collections.singletonList(
                    DataTypes.createStructField("nested", DataTypes.StringType, true)
                )
            ),
            Row.class
        },
        {
          "map_int",
            DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true),
            scala.collection.Map.class
        },
        { "repeating_message",
            DataTypes.createArrayType(
                DataTypes.createStructType(
                    Collections.singletonList(
                        DataTypes.createStructField("nested", DataTypes.StringType,true)
                    )
                ), true),
            Seq.class
        },
        { "repeating_int32",
            DataTypes.createArrayType(DataTypes.IntegerType, true),
            Seq.class
        },
        { "repeating_enum",
            DataTypes.createArrayType(DataTypes.StringType, true),
            Seq.class
        }
    });
  }

  private String field;
  private DataType dataType;
  private Class aClass;

  public TestProtobufUtilsConversion(String field, DataType dataType, Class aClass) {
    this.field = field;
    this.dataType = dataType;
    this.aClass = aClass;
  }

  @Test
  public void convertType() {
    assertThat(ProtobufUtils.convertType(SINGLE_DESC.findFieldByName(field)), is(dataType));
  }

  @Test
  public void getFieldValue() {
    assertThat(ProtobufUtils.getFieldValue(SINGLE_DESC.findFieldByName(field), message.getField(
        SINGLE_DESC.findFieldByName(field)), dataType), instanceOf(aClass));
  }

}
