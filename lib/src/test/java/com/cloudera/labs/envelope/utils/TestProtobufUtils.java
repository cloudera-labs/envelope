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
import com.cloudera.labs.envelope.input.translate.ProtobufSingleMessage;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.collection.IsIterableContainingInRelativeOrder.containsInRelativeOrder;
import org.junit.Assert;
import static org.junit.Assert.assertThat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import scala.collection.JavaConversions;

public class TestProtobufUtils {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final String SINGLE_EXAMPLE = "/protobuf/protobuf_single_message.desc";
  private static final String MULTIPLE_EXAMPLE = "/protobuf/protobuf_multiple_message.desc";
  private static final Descriptors.Descriptor SINGLE_DESC;

  static {
    SINGLE_DESC = marshalDescriptor(SINGLE_EXAMPLE);
  }

  private static Descriptors.Descriptor marshalDescriptor(String desc) {
    DescriptorProtos.FileDescriptorSet fileDescriptorSet;

    try (BufferedInputStream dscFile = new BufferedInputStream(new FileInputStream(
        TestProtobufUtils.class.getResource(desc).getPath()
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

      return fileDescriptor.getMessageTypes().get(0);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException("Error constructing FileDescriptor", e);
    }
  }

  public static final StructType SINGLE_SCHEMA = DataTypes.createStructType(Arrays.asList(
      DataTypes.createStructField("string", DataTypes.StringType, true),
      DataTypes.createStructField("double", DataTypes.DoubleType, true),
      DataTypes.createStructField("float", DataTypes.FloatType, true),
      DataTypes.createStructField("int32", DataTypes.IntegerType, true),
      DataTypes.createStructField("int64", DataTypes.LongType, true),
      DataTypes.createStructField("uint32", DataTypes.IntegerType, true),
      DataTypes.createStructField("uint64", DataTypes.LongType, true),
      DataTypes.createStructField("sint32", DataTypes.IntegerType, true),
      DataTypes.createStructField("sint64", DataTypes.LongType, true),
      DataTypes.createStructField("fixed32", DataTypes.IntegerType, true),
      DataTypes.createStructField("fixed64", DataTypes.LongType, true),
      DataTypes.createStructField("sfixed32", DataTypes.IntegerType, true),
      DataTypes.createStructField("sfixed64", DataTypes.LongType, true),
      DataTypes.createStructField("boolean", DataTypes.BooleanType, true),
      DataTypes.createStructField("bytes", DataTypes.BinaryType, true),
      DataTypes.createStructField("enum", DataTypes.StringType, true),
      DataTypes.createStructField("nested", DataTypes.createStructType(
          Collections.singletonList(
              DataTypes.createStructField("nested", DataTypes.StringType, true)
          )
      ), true),
      DataTypes.createStructField("map_int", DataTypes.createMapType(
          DataTypes.StringType, DataTypes.IntegerType, true
      ), true),
      DataTypes.createStructField("repeating_message", DataTypes.createArrayType(
          DataTypes.createStructType(Collections.singletonList(
              DataTypes.createStructField("nested", DataTypes.StringType, true)
          )), true
      ), true),
      DataTypes.createStructField("repeating_int32", DataTypes.createArrayType(
          DataTypes.IntegerType, true
      ), true),
      DataTypes.createStructField("repeating_enum", DataTypes.createArrayType(
          DataTypes.StringType, true
      ), true),
      DataTypes.createStructField("oneof_string", DataTypes.StringType, true),
      DataTypes.createStructField("oneof_int", DataTypes.IntegerType, true),
      DataTypes.createStructField("oneof_nested", DataTypes.createStructType(
          Collections.singletonList(DataTypes.createStructField("nested", DataTypes.StringType, true))
      ), true)
  ));

  @Test
  public void buildRowValues() {
    DynamicMessage nestedMessage0 = DynamicMessage.newBuilder(SINGLE_DESC.findNestedTypeByName("NestedExample"))
        .setField(SINGLE_DESC.findNestedTypeByName("NestedExample").findFieldByName("nested"), "a nested type")
        .build();

    DynamicMessage nestedMessage1 = DynamicMessage.newBuilder(SINGLE_DESC.findNestedTypeByName("NestedExample"))
        .setField(SINGLE_DESC.findNestedTypeByName("NestedExample").findFieldByName("nested"), "another nested type")
        .build();

    DynamicMessage mapInt0 = DynamicMessage.newBuilder(SINGLE_DESC.findNestedTypeByName("MapIntEntry"))
        .setField(SINGLE_DESC.findNestedTypeByName("MapIntEntry").findFieldByName("key"), "key value")
        .setField(SINGLE_DESC.findNestedTypeByName("MapIntEntry").findFieldByName("value"), 987)
        .build();

    DynamicMessage mapInt1 = DynamicMessage.newBuilder(SINGLE_DESC.findNestedTypeByName("MapIntEntry"))
        .setField(SINGLE_DESC.findNestedTypeByName("MapIntEntry").findFieldByName("key"), "another key value")
        .setField(SINGLE_DESC.findNestedTypeByName("MapIntEntry").findFieldByName("value"), 123)
        .build();

    Map<String, Integer> stringIntegerMap = new HashMap<>();
    stringIntegerMap.put("key value", 987);
    stringIntegerMap.put("another key value", 123);

    DynamicMessage message = DynamicMessage.newBuilder(SINGLE_DESC)
        .setField(SINGLE_DESC.findFieldByName("string"), "string value")
        .setField(SINGLE_DESC.findFieldByName("double"), 987.654D)
        .setField(SINGLE_DESC.findFieldByName("float"), 987.654F)
        .setField(SINGLE_DESC.findFieldByName("int32"), 987)
        .setField(SINGLE_DESC.findFieldByName("int64"), 987L)
        .setField(SINGLE_DESC.findFieldByName("uint32"), 987)
        .setField(SINGLE_DESC.findFieldByName("uint64"), 987L)
        .setField(SINGLE_DESC.findFieldByName("sint32"), 987)
        .setField(SINGLE_DESC.findFieldByName("sint64"), 987L)
        .setField(SINGLE_DESC.findFieldByName("fixed32"), 987)
        .setField(SINGLE_DESC.findFieldByName("fixed64"), 987L)
        .setField(SINGLE_DESC.findFieldByName("sfixed32"), 987)
        .setField(SINGLE_DESC.findFieldByName("sfixed64"), 987L)
        .setField(SINGLE_DESC.findFieldByName("boolean"), true)
        .setField(SINGLE_DESC.findFieldByName("bytes"), ByteString.copyFrom("foo".getBytes()))
        .setField(SINGLE_DESC.findFieldByName("enum"), SINGLE_DESC.findEnumTypeByName("EnumExample")
            .findValueByName("THREE"))
        .setField(SINGLE_DESC.findFieldByName("nested"), nestedMessage0)
        .addRepeatedField(SINGLE_DESC.findFieldByName("map_int"), mapInt0)
        .addRepeatedField(SINGLE_DESC.findFieldByName("map_int"), mapInt1)
        .addRepeatedField(SINGLE_DESC.findFieldByName("repeating_message"), nestedMessage0)
        .addRepeatedField(SINGLE_DESC.findFieldByName("repeating_message"), nestedMessage1)
        .addRepeatedField(SINGLE_DESC.findFieldByName("repeating_int32"), 1234)
        .addRepeatedField(SINGLE_DESC.findFieldByName("repeating_int32"), 5678)
        .addRepeatedField(SINGLE_DESC.findFieldByName("repeating_enum"), SINGLE_DESC.findEnumTypeByName("EnumExample")
            .findValueByName("TWO"))
        .addRepeatedField(SINGLE_DESC.findFieldByName("repeating_enum"), SINGLE_DESC.findEnumTypeByName("EnumExample")
            .findValueByName("TWO"))
        .build();

    List<Object> results = ProtobufUtils.buildRowValues(SINGLE_DESC, message, SINGLE_SCHEMA);

    assertThat(results, contains(
        (Object) "string value",
        987.654D,
        987.654F,
        987,
        987L,
        987,
        987L,
        987,
        987L,
        987,
        987L,
        987,
        987L,
        true,
        "foo".getBytes(),
        "THREE",
        RowFactory.create("a nested type"),
        JavaConversions.mapAsScalaMap(stringIntegerMap),
        JavaConversions.asScalaBuffer(Arrays.asList(RowFactory.create("a nested type"), RowFactory.create("another nested type"))),
        JavaConversions.asScalaBuffer(Arrays.asList(1234, 5678)),
        JavaConversions.asScalaBuffer(Arrays.asList("TWO", "TWO")),
        null,
        null,
        null
        )
    );
  }

  @Test
  public void buildRowValuesSparse() {
    DynamicMessage message = DynamicMessage.newBuilder(SINGLE_DESC)
        .setField(SINGLE_DESC.findFieldByName("string"), "string value")
        .build();

    List<Object> results = ProtobufUtils.buildRowValues(SINGLE_DESC, message, SINGLE_SCHEMA);

    assertThat(results, containsInRelativeOrder((Object) "string value", null));
    assertThat(results.size(), is(24));
  }

  @Test
  public void buildRowValuesIndexed() {
    DynamicMessage message = DynamicMessage.newBuilder(SINGLE_DESC)
        .setField(SINGLE_DESC.findFieldByName("boolean"), true)
        .build();

    List<Object> results = ProtobufUtils.buildRowValues(SINGLE_DESC, message, SINGLE_SCHEMA);

    assertThat(results, containsInRelativeOrder((Object) true, null));
    assertThat(results.size(), is(24));
    assertThat(results.indexOf((Object) true), is(13));
  }

  // See ProtobufUtilsConversionTest for other tests
  @Test
  public void getFieldValueNull() {
    Assert.assertThat(ProtobufUtils.getFieldValue(SINGLE_DESC.findFieldByName("string"), null,
        SINGLE_SCHEMA.apply("string").dataType()), nullValue());
  }

  @Test
  public void buildSchemaSingle() {
    Descriptors.Descriptor descriptor = ProtobufSingleMessage.SingleExample.getDescriptor();
    Assert.assertThat(ProtobufUtils.buildSchema(descriptor), is(SINGLE_SCHEMA));
  }

  @Test
  public void buildDescriptorSingleNoDefinedName() {
    Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(
        ProtobufUtils.class.getResource(SINGLE_EXAMPLE).getPath());

    Assert.assertThat(descriptor.getName(), is("SingleExample"));
    Assert.assertThat(descriptor.getFields().size(), is(24));
    Assert.assertThat(descriptor.getEnumTypes().size(), is(1));
    Assert.assertThat(descriptor.getNestedTypes().size(), is(2)); // Map is converted to a nested type
  }

  @Test
  public void buildDescriptorSingleDefinedName() {
    Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(
        ProtobufUtils.class.getResource(SINGLE_EXAMPLE).getPath(), "SingleExample");

    Assert.assertThat(descriptor.getName(), is("SingleExample"));
    Assert.assertThat(descriptor.getFields().size(), is(24));
    Assert.assertThat(descriptor.getEnumTypes().size(), is(1));
    Assert.assertThat(descriptor.getNestedTypes().size(), is(2)); // Map is converted to a nested type
  }

  @Test
  public void buildDescriptorMultipleNoDefinedName() {
    thrown.expect(RuntimeException.class);
    ProtobufUtils.buildDescriptor(ProtobufUtils.class.getResource(MULTIPLE_EXAMPLE).getPath());
  }

  @Test
  public void buildDescriptorMultipleInvalidName() {
    thrown.expect(RuntimeException.class);
    ProtobufUtils.buildDescriptor(ProtobufUtils.class.getResource(MULTIPLE_EXAMPLE).getPath(), "None");
  }

  @Test
  public void buildDescriptorMultipleDefinedName() {
    Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(
        ProtobufUtils.class.getResource(MULTIPLE_EXAMPLE).getPath(), "MultipleExample");

    Assert.assertThat(descriptor.getName(), is("MultipleExample"));
    Assert.assertThat(descriptor.getFields().size(), is(1));
    Assert.assertThat(descriptor.getFields().get(0).getType(), is(Descriptors.FieldDescriptor.Type.MESSAGE));
  }

}