/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.common.base.Preconditions;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;

public class ProtobufDynamicMessageSchema {
  private final Descriptors.Descriptor descriptor;
  private final Schema schema;
  private final List<Converter> converters;

  public ProtobufDynamicMessageSchema(Descriptors.Descriptor descriptor) {
    this(descriptor, ProtoSchemaTranslator.getSchema(descriptor));
  }

  public ProtobufDynamicMessageSchema(Descriptors.Descriptor descriptor, Schema schema) {
    this.descriptor = descriptor;
    this.schema = schema;
    this.converters = createConverters(descriptor, schema);
  }

  @NotNull
  private static List<Converter> createConverters(
      Descriptors.Descriptor descriptor, Schema schema) {
    return schema.getFields().stream()
        .map(
            field -> {
              if (field.getType().isLogicalType(OneOfType.IDENTIFIER)) {
                return createOneOfConverter(
                    descriptor, field.getType().getLogicalType(OneOfType.class));
              } else {
                return createConverter(
                    descriptor.findFieldByName(field.getName()), field.getType());
              }
            })
        .collect(Collectors.toList());
  }

  public static Converter createOneOfConverter(
      Descriptors.Descriptor protoDescriptor, OneOfType oneOfType) {
    return new OneOfConverter(protoDescriptor, oneOfType);
  }

  public static Converter createConverter(
      Descriptors.FieldDescriptor protoFieldDescriptor, Schema.FieldType beamFieldType) {

    switch (beamFieldType.getTypeName()) {
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BOOLEAN:
        return new PrimitiveConverter(protoFieldDescriptor);
      case BYTES:
        return new BytesConverter(protoFieldDescriptor);
      case DATETIME:
        throw new RuntimeException();
      case ARRAY:
      case ITERABLE:
        return new ArrayConverter(
            protoFieldDescriptor,
            createConverter(
                protoFieldDescriptor,
                Preconditions.checkNotNull(
                    beamFieldType.getCollectionElementType(),
                    "Beam field type '%s' doesn't have element type",
                    beamFieldType)));
      case MAP:
        return new MapConverter(
            protoFieldDescriptor,
            createConverter(
                protoFieldDescriptor.getMessageType().findFieldByName("key"),
                Preconditions.checkNotNull(
                    beamFieldType.getMapKeyType(),
                    "Beam field type '%s' doesn't have key type",
                    beamFieldType)),
            createConverter(
                protoFieldDescriptor.getMessageType().findFieldByName("value"),
                Preconditions.checkNotNull(
                    beamFieldType.getMapValueType(),
                    "Beam field type '%s' doesn't have value type",
                    beamFieldType)));
      case ROW:
        return new RowConverter(
            protoFieldDescriptor, Preconditions.checkNotNull(beamFieldType.getRowSchema()));
      case LOGICAL_TYPE:
        String identifier =
            Preconditions.checkNotNull(beamFieldType.getLogicalType()).getIdentifier();
        switch (identifier) {
          case ProtoSchemaLogicalTypes.Fixed32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.Fixed64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SFixed32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SFixed64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SInt32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SInt64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.UInt32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.UInt64.IDENTIFIER:
            return new ProtoSchemaLogicalTypeConverter(
                protoFieldDescriptor, beamFieldType.getLogicalType(PassThroughLogicalType.class));
          case NanosInstant.IDENTIFIER:
            return new TimestampConverter(protoFieldDescriptor);
          case NanosDuration.IDENTIFIER:
            return new DurationConverter(protoFieldDescriptor);
          case EnumerationType.IDENTIFIER:
            return new EnumConverter(
                protoFieldDescriptor, beamFieldType.getLogicalType(EnumerationType.class));
          default:
            throw new IllegalStateException("Unexpected logical type : " + beamFieldType);
        }
      default:
        throw new RuntimeException(
            String.format("Unsupported Beam FieldType '%s' to convert to Protobuf", beamFieldType));
    }
  }

  /**
   * Gets the proto field's value. If the value is {@code null}, this method returns {@code null}
   * for a nullable field, or the primitive default value otherwise.
   */
  private static @Nullable Object getProtoField(@NonNull Message protoMessage, int number) {
    Descriptors.FieldDescriptor realFieldDescriptor =
        protoMessage.getDescriptorForType().findFieldByNumber(number);
    if (ProtoSchemaTranslator.isNullable(realFieldDescriptor)
        && !protoMessage.hasField(realFieldDescriptor)) {
      return null;
    } else {
      return protoMessage.getField(realFieldDescriptor);
    }
  }

  private static Object unwrapScalarIfNecessary(
      Descriptors.FieldDescriptor fieldDescriptor, Object protoObject) {
    if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
      Preconditions.checkArgument(protoObject instanceof Message);
      Message message = (Message) protoObject;
      Descriptors.FieldDescriptor wrapperValueFieldDescriptor =
          message.getDescriptorForType().findFieldByNumber(1);
      Preconditions.checkArgument(
          wrapperValueFieldDescriptor.getName().equals("value"),
          "not a wrapper: %s",
          wrapperValueFieldDescriptor);
      return message.getField(wrapperValueFieldDescriptor);
    } else {
      Preconditions.checkArgument(!(protoObject instanceof Message));
      return protoObject;
    }
  }

  private static Object wrapScalarIfNecessary(
      Descriptors.FieldDescriptor fieldDescriptor, Object protoObject) {
    Preconditions.checkArgument(!(protoObject instanceof Message));
    if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
      Descriptors.Descriptor messageType = fieldDescriptor.getMessageType();
      DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(messageType);
      messageBuilder.setField(messageType.getFields().get(0), protoObject);
      return messageBuilder.build();
    } else {
      return protoObject;
    }
  }

  public DynamicMessage toProtoMessage(Row row) {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    for (int i = 0; i < converters.size(); i++) {
      converters.get(i).setFieldToProtoMessage(builder, row.getValue(i));
    }
    return builder.build();
  }

  public Row toRow(Message protoMessage) {
    Row.Builder builder = Row.withSchema(schema);
    for (Converter converter : converters) {
      Object fieldFromProtoMessage = converter.getFieldFromProtoMessage(protoMessage);
      builder.addValue(fieldFromProtoMessage);
    }
    return builder.build();
  }

  public interface Converter {
    @Nullable
    Object convertFromProtoObject(@Nullable Object protoObject);

    @Nullable
    Object convertToProtoObject(@Nullable Object beamObject);

    @Nullable
    Object getFieldFromProtoMessage(@NonNull Message protoMessage);

    void setFieldToProtoMessage(
        Message.Builder protoMesageBuilder, @Nullable Object beamFieldValue);
  }

  abstract static class FieldConverter implements Converter {
    protected final Descriptors.FieldDescriptor fieldDescriptor;

    FieldConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    @Nullable
    public Object convertFromProtoObject(@Nullable Object protoObject) {
      return protoObject != null ? convertFromNonNullProtoObject(protoObject) : null;
    }

    @Override
    @Nullable
    public Object convertToProtoObject(@Nullable Object beamObject) {
      return beamObject != null ? convertNonNullToProtoObject(beamObject) : null;
    }

    @Override
    @Nullable
    public Object getFieldFromProtoMessage(@NonNull Message protoMessage) {
      return convertFromProtoObject(getProtoField(protoMessage, fieldDescriptor.getNumber()));
    }

    @Override
    public void setFieldToProtoMessage(
        Message.Builder protoMesageBuilder, @Nullable Object beamFieldValue) {
      if (beamFieldValue == null) {
        return;
      }
      protoMesageBuilder.setField(fieldDescriptor, convertNonNullToProtoObject(beamFieldValue));
    }

    abstract Object convertFromNonNullProtoObject(@NonNull Object protoObject);

    abstract Object convertNonNullToProtoObject(@NonNull Object protoObject);
  }

  static class PrimitiveConverter extends FieldConverter implements Serializable {
    PrimitiveConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public @NonNull Object convertFromNonNullProtoObject(@NonNull Object protoObject) {
      return unwrapScalarIfNecessary(fieldDescriptor, protoObject);
    }

    @Override
    public @NonNull Object convertNonNullToProtoObject(@NonNull Object beamObject) {
      return wrapScalarIfNecessary(fieldDescriptor, beamObject);
    }
  }

  static class ArrayConverter extends FieldConverter {
    private final Converter elementConverter;

    ArrayConverter(Descriptors.FieldDescriptor fieldDescriptor, Converter elementConverter) {
      super(fieldDescriptor);
      this.elementConverter = elementConverter;
    }

    @Override
    public @NonNull List<@Nullable Object> convertFromNonNullProtoObject(
        @NonNull Object protoObject) {
      return ((Collection<@Nullable Object>) protoObject)
          .stream()
              .<@Nullable Object>map(elementConverter::convertFromProtoObject)
              .collect(Collectors.toList());
    }

    @Override
    public @NonNull List<@Nullable Object> convertNonNullToProtoObject(@NonNull Object beamObject) {
      return ((Collection<@Nullable Object>) beamObject)
          .stream()
              .<@Nullable Object>map(elementConverter::convertToProtoObject)
              .collect(Collectors.toList());
    }
  }

  static class MapConverter extends FieldConverter {
    private final Descriptors.FieldDescriptor keyDescriptor;
    private final Descriptors.FieldDescriptor valueDescriptor;
    private final Converter keyConverter;
    private final Converter valueConverter;

    MapConverter(
        Descriptors.FieldDescriptor fieldDescriptor,
        Converter keyConverter,
        Converter valueConverter) {
      super(fieldDescriptor);
      this.keyDescriptor = fieldDescriptor.getMessageType().findFieldByName("key");
      this.valueDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
      this.keyConverter = keyConverter;
      this.valueConverter = valueConverter;
    }

    @Override
    public @NonNull Map<Object, Object> convertFromNonNullProtoObject(@NonNull Object protoObject) {
      List<Message> protoList = (List<Message>) protoObject;
      Map<Object, Object> beamMap = new HashMap<>();
      protoList.forEach(
          message ->
              beamMap.put(
                  Preconditions.checkNotNull(
                      keyConverter.convertFromProtoObject(
                          getProtoField(message, keyDescriptor.getNumber()))),
                  Preconditions.checkNotNull(
                      valueConverter.convertFromProtoObject(
                          getProtoField(message, valueDescriptor.getNumber())))));
      return beamMap;
    }

    @Override
    public @NonNull List<Message> convertNonNullToProtoObject(@NonNull Object beamObject) {
      List<Message> protoList = new ArrayList<>();
      ((Map<Object, Object>) beamObject)
          .forEach(
              (k, v) ->
                  protoList.add(
                      DynamicMessage.newBuilder(fieldDescriptor.getMessageType())
                          .setField(
                              keyDescriptor,
                              Preconditions.checkNotNull(keyConverter.convertToProtoObject(k)))
                          .setField(
                              valueDescriptor,
                              Preconditions.checkNotNull(valueConverter.convertToProtoObject(v)))
                          .build()));
      return protoList;
    }
  }

  static class RowConverter extends FieldConverter {
    private final Schema schema;

    RowConverter(Descriptors.FieldDescriptor fieldDescriptor, Schema schema) {
      super(fieldDescriptor);
      this.schema = schema;
    }

    @Override
    public @NonNull Row convertFromNonNullProtoObject(@NonNull Object protoObject) {
      Preconditions.checkArgument(protoObject instanceof Message);
      return new ProtobufDynamicMessageSchema(fieldDescriptor.getMessageType(), schema)
          .toRow((Message) protoObject);
    }

    @Override
    public @NonNull Message convertNonNullToProtoObject(@NonNull Object beamObject) {
      return new ProtobufDynamicMessageSchema(fieldDescriptor.getMessageType(), schema)
          .toProtoMessage((Row) beamObject);
    }
  }

  static class BytesConverter extends FieldConverter {

    BytesConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public byte[] convertFromNonNullProtoObject(@NonNull Object protoObject) {
      return ((ByteString) unwrapScalarIfNecessary(fieldDescriptor, protoObject)).toByteArray();
    }

    @Override
    public @NonNull Object convertNonNullToProtoObject(@NonNull Object beamObject) {
      return wrapScalarIfNecessary(fieldDescriptor, ByteString.copyFrom((byte[]) beamObject));
    }
  }

  static class ProtoSchemaLogicalTypeConverter extends FieldConverter {
    private final Schema.LogicalType<Object, Object> logicalType;

    ProtoSchemaLogicalTypeConverter(
        Descriptors.FieldDescriptor fieldDescriptor,
        Schema.LogicalType<Object, Object> logicalType) {
      super(fieldDescriptor);
      this.logicalType = logicalType;
    }

    @Override
    @NonNull
    public Object convertFromNonNullProtoObject(@NonNull Object protoObject) {
      return logicalType.toBaseType(unwrapScalarIfNecessary(fieldDescriptor, protoObject));
    }

    @Override
    @NonNull
    public Object convertNonNullToProtoObject(@NonNull Object beamObject) {
      return wrapScalarIfNecessary(fieldDescriptor, logicalType.toInputType(beamObject));
    }
  }

  static class TimestampConverter extends FieldConverter {
    TimestampConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    @NonNull
    public Object convertFromNonNullProtoObject(@NonNull Object protoObject) {
      Message protoTimestamp = (Message) protoObject;
      Descriptors.FieldDescriptor secondsFieldDescriptor =
          protoTimestamp.getDescriptorForType().findFieldByNumber(1);
      Descriptors.FieldDescriptor nanosFieldDescriptor =
          protoTimestamp.getDescriptorForType().findFieldByNumber(2);
      return Instant.ofEpochSecond(
          (long) protoTimestamp.getField(secondsFieldDescriptor),
          (int) protoTimestamp.getField(nanosFieldDescriptor));
    }

    @Override
    @NonNull
    public Object convertNonNullToProtoObject(@NonNull Object beamObject) {
      Instant ts = (Instant) beamObject;
      return com.google.protobuf.Timestamp.newBuilder()
          .setSeconds(ts.getEpochSecond())
          .setNanos(ts.getNano())
          .build();
    }
  }

  static class DurationConverter extends FieldConverter {
    DurationConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    @NonNull
    public Object convertFromNonNullProtoObject(@NonNull Object protoObject) {
      Message protoDuration = (Message) protoObject;
      Descriptors.FieldDescriptor secondsFieldDescriptor =
          protoDuration.getDescriptorForType().findFieldByNumber(1);
      Descriptors.FieldDescriptor nanosFieldDescriptor =
          protoDuration.getDescriptorForType().findFieldByNumber(2);
      return Duration.ofSeconds(
          (long) protoDuration.getField(secondsFieldDescriptor),
          (int) protoDuration.getField(nanosFieldDescriptor));
    }

    @Override
    @NonNull
    public Object convertNonNullToProtoObject(@NonNull Object beamObject) {
      Duration duration = (Duration) beamObject;
      return com.google.protobuf.Duration.newBuilder()
          .setSeconds(duration.getSeconds())
          .setNanos(duration.getNano())
          .build();
    }
  }

  static class EnumConverter extends FieldConverter {
    private final EnumerationType logicalType;

    EnumConverter(Descriptors.FieldDescriptor fieldDescriptor, EnumerationType logicalType) {
      super(fieldDescriptor);
      this.logicalType = logicalType;
    }

    @Override
    @NonNull
    public Object convertFromNonNullProtoObject(@NonNull Object protoObject) {
      Descriptors.EnumValueDescriptor enumDescriptor =
          (Descriptors.EnumValueDescriptor) protoObject;
      return logicalType.toInputType(enumDescriptor.getNumber());
    }

    @Override
    @NonNull
    public Object convertNonNullToProtoObject(@NonNull Object beamObject) {
      return fieldDescriptor
          .getEnumType()
          .findValueByNumber(logicalType.toBaseType((EnumerationType.Value) beamObject));
    }
  }

  static class OneOfConverter implements Converter {
    private final OneOfType oneOfType;
    private final Map<Integer, Converter> converters;

    OneOfConverter(Descriptors.Descriptor descriptor, OneOfType oneOfType) {
      this.oneOfType = oneOfType;
      ImmutableMap.Builder<Integer, Converter> convertersBuilder = ImmutableMap.builder();
      oneOfType
          .getOneOfSchema()
          .getFields()
          .forEach(
              beamField -> {
                int protoFieldNumber = ProtoSchemaTranslator.getFieldNumber(beamField);
                Descriptors.FieldDescriptor fieldDescriptor =
                    descriptor.findFieldByNumber(protoFieldNumber);
                convertersBuilder.put(
                    protoFieldNumber, createConverter(fieldDescriptor, beamField.getType()));
              });
      converters = convertersBuilder.build();
    }

    @Override
    @Nullable
    public Object getFieldFromProtoMessage(@NonNull Message protoMessage) {
      for (Map.Entry<Integer, Converter> converter : converters.entrySet()) {
        Object value = converter.getValue().getFieldFromProtoMessage(protoMessage);
        if (value != null) {
          return oneOfType.createValue(converter.getKey(), value);
        }
      }
      return null;
    }

    @Override
    public void setFieldToProtoMessage(
        Message.Builder protoMesageBuilder, @Nullable Object beamFieldValue) {
      if (beamFieldValue == null) {
        return;
      }

      OneOfType.Value oneOfValue = (OneOfType.Value) beamFieldValue;
      int protoFieldNumber = oneOfValue.getCaseType().getValue();
      Preconditions.checkNotNull(converters.get(protoFieldNumber))
          .setFieldToProtoMessage(protoMesageBuilder, oneOfValue.getValue());
    }

    @Override
    @Nullable
    public Object convertFromProtoObject(@Nullable Object protoObject) {
      throw new UnsupportedOperationException();
    }

    @Override
    @Nullable
    public Object convertToProtoObject(@Nullable Object beamObject) {
      throw new UnsupportedOperationException();
    }
  }
}
