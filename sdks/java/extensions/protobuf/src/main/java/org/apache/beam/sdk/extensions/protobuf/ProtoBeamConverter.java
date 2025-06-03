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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;

public class ProtoBeamConverter {

  public static SerializableFunction<@NonNull Row, @NonNull Message> toProto(
      Descriptors.Descriptor descriptor) {
    Map<String, ToProto<?>> toProtos = new LinkedHashMap<>();
    for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
      if (fieldDescriptor.getRealContainingOneof() != null) {
        Descriptors.OneofDescriptor realContainingOneof = fieldDescriptor.getRealContainingOneof();
        if (realContainingOneof.getField(0) == fieldDescriptor) {
          toProtos.put(realContainingOneof.getName(), new ToProtoOneOf(realContainingOneof));
        }
        // continue
      } else if (fieldDescriptor.isRepeated()) {
        if (fieldDescriptor.isMapField()) {
          toProtos.put(fieldDescriptor.getName(), new ToProtoMapToRepeated<>(fieldDescriptor));
        } else {
          toProtos.put(fieldDescriptor.getName(), new ToProtoIterableToRepeated<>(fieldDescriptor));
        }
      } else {
        toProtos.put(fieldDescriptor.getName(), createToProtoSingular(fieldDescriptor));
      }
    }
    return row -> {
      DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor);
      for (Map.Entry<String, ToProto<?>> entry : toProtos.entrySet()) {
        String fieldName = entry.getKey();
        @SuppressWarnings("unchecked")
        ToProto<Object> converter = (ToProto<Object>) entry.getValue();
        converter.setProtoField(message, row.getValue(fieldName));
      }
      return message.build();
    };
  }

  static Descriptors.FieldDescriptor findDescriptor(
      MessageOrBuilder message, Descriptors.FieldDescriptor fieldDescriptor) {
    Descriptors.Descriptor messageDescriptor = message.getDescriptorForType();
    Descriptors.FieldDescriptor messageFieldDescriptor =
        messageDescriptor.findFieldByNumber(fieldDescriptor.getNumber());
    Preconditions.checkState(messageFieldDescriptor.getName().equals(fieldDescriptor.getName()));
    return messageFieldDescriptor;
  }

  static ToProtoField<?> createToProtoSingular(Descriptors.FieldDescriptor fieldDescriptor) {
    switch (fieldDescriptor.getJavaType()) {
      case INT:
        return new ToProtoPassThrough<>(fieldDescriptor, 0);
      case LONG:
        return new ToProtoPassThrough<>(fieldDescriptor, 0L);
      case FLOAT:
        return new ToProtoPassThrough<>(fieldDescriptor, 0f);
      case DOUBLE:
        return new ToProtoPassThrough<>(fieldDescriptor, 0.0);
      case BOOLEAN:
        return new ToProtoPassThrough<>(fieldDescriptor, false);
      case STRING:
        return new ToProtoPassThrough<>(fieldDescriptor, "");
      case BYTE_STRING:
        return new ToProtoByteString(fieldDescriptor);
      case ENUM:
        return new ToProtoEnum(fieldDescriptor);
      case MESSAGE:
        String fullName = fieldDescriptor.getMessageType().getFullName();
        switch (fullName) {
          case "google.protobuf.Int32Value":
          case "google.protobuf.UInt32Value":
            return new ToProtoPassThrough<>(fieldDescriptor, 0);
          case "google.protobuf.Int64Value":
          case "google.protobuf.UInt64Value":
            return new ToProtoPassThrough<>(fieldDescriptor, 0L);
          case "google.protobuf.FloatValue":
            return new ToProtoPassThrough<>(fieldDescriptor, 0f);
          case "google.protobuf.DoubleValue":
            return new ToProtoPassThrough<>(fieldDescriptor, 0.0);
          case "google.protobuf.StringValue":
            return new ToProtoPassThrough<>(fieldDescriptor, "");
          case "google.protobuf.BoolValue":
            return new ToProtoPassThrough<>(fieldDescriptor, false);
          case "google.protobuf.BytesValue":
            return new ToProtoByteString(fieldDescriptor);
          case "google.protobuf.Timestamp":
            return new ToProtoTimestamp(fieldDescriptor);
          case "google.protobuf.Duration":
            return new ToProtoDuration(fieldDescriptor);
          case "google.protobuf.Any":
            throw new UnsupportedOperationException("google.protobuf.Any is not supported");
          default:
            return new ToProtoMessage(fieldDescriptor);
        }
      default:
        throw new UnsupportedOperationException(
            "Unsupported proto type: " + fieldDescriptor.getJavaType());
    }
  }

  public static SerializableFunction<@NonNull Message, @NonNull Row> fromProto(Schema schema) {
    List<FromProto<?>> toBeams = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      Schema.FieldType fieldType = field.getType();
      if (fieldType.isLogicalType(OneOfType.IDENTIFIER)) {
        toBeams.add(new FromProtoOneOf(fieldType));
      } else {
        toBeams.add(new FromProtoField<>(field));
      }
    }

    return message -> {
      Row.Builder rowBuilder = Row.withSchema(schema);
      for (FromProto<?> toBeam : toBeams) {
        rowBuilder.addValue(toBeam.getBeamField(message));
      }
      return rowBuilder.build();
    };
  }

  static BeamConverter<?, ?> createBeamConverter(Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case BYTE:
        throw new UnsupportedOperationException();
      case INT16:
        throw new UnsupportedOperationException();
      case INT32:
        return new BeamPassThroughConverter<>(fieldType, 0);
      case INT64:
        return new BeamPassThroughConverter<>(fieldType, 0L);
      case DECIMAL:
        throw new UnsupportedOperationException();
      case FLOAT:
        return new BeamPassThroughConverter<>(fieldType, 0f);
      case DOUBLE:
        return new BeamPassThroughConverter<>(fieldType, 0.0);
      case STRING:
        return new BeamPassThroughConverter<>(fieldType, "");
      case DATETIME:
        throw new UnsupportedOperationException();
      case BOOLEAN:
        return new BeamPassThroughConverter<>(fieldType, false);
      case BYTES:
        return new BeamBytesConverter(fieldType);
      case ARRAY:
      case ITERABLE:
        return new BeamListConverter<>(fieldType);
      case MAP:
        return new BeamMapConverter<>(fieldType);
      case ROW:
        return new BeamRowConverter(fieldType);
      case LOGICAL_TYPE:
        switch (Preconditions.checkNotNull(fieldType.getLogicalType()).getIdentifier()) {
          case ProtoSchemaLogicalTypes.UInt32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SInt32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.Fixed32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SFixed32.IDENTIFIER:
            return new BeamPassThroughConverter<>(fieldType, 0);
          case ProtoSchemaLogicalTypes.UInt64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SInt64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.Fixed64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SFixed64.IDENTIFIER:
            return new BeamPassThroughConverter<>(fieldType, 0L);
          case NanosDuration.IDENTIFIER:
            return new BeamNanosDurationConverter(fieldType);
          case NanosInstant.IDENTIFIER:
            return new BeamNanosInstantConverter(fieldType);
          case EnumerationType.IDENTIFIER:
            return new BeamEnumerationConverter(fieldType);
          default:
            throw new UnsupportedOperationException();
        }
      default:
        throw new UnsupportedOperationException();
    }
  }

  interface ToProto<B> {
    void setProtoField(Message.Builder message, B beamFieldValue);
  }

  interface FromProto<B> {
    @Nullable
    B getBeamField(Message message);
  }

  static class FromProtoField<P, B> implements FromProto<B> {
    private final Schema.Field field;
    private final BeamConverter<P, B> converter;

    @SuppressWarnings("unchecked")
    FromProtoField(Schema.Field field) {
      this.field = field;
      converter = (BeamConverter<P, B>) createBeamConverter(field.getType());
    }

    @SuppressWarnings("unchecked")
    @Override
    public @Nullable B getBeamField(Message message) {
      Descriptors.Descriptor descriptor = message.getDescriptorForType();
      Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(field.getName());
      @Nullable Object protoValue = message.getField(fieldDescriptor);
      return converter.convert((P) protoValue);
    }
  }

  abstract static class ToProtoField<B> implements ToProto<B> {
    protected final Descriptors.FieldDescriptor fieldDescriptor;

    ToProtoField(Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
    }

    public abstract @Nullable Object convert(@Nullable B beamValue);
  }

  abstract static class ToProtoWrappable<B, P> extends ToProtoField<B> {
    ToProtoWrappable(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public void setProtoField(Message.Builder message, B beamFieldValue) {
      Object protoValue = convert(beamFieldValue);
      if (protoValue != null) {
        Descriptors.FieldDescriptor messageFieldDescriptor =
            findDescriptor(message, fieldDescriptor);
        message.setField(messageFieldDescriptor, protoValue);
      }
    }

    @Override
    @Nullable
    public Object convert(@Nullable B beamValue) {
      if (ProtoSchemaTranslator.isNullable(fieldDescriptor) && beamValue == null) {
        return null;
      }

      P protoValue = beamValue != null ? convertNonNullUnwrapped(beamValue) : defaultValue();
      if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
        // A google.protobuf wrapper
        Descriptors.Descriptor wrapperDescriptor = fieldDescriptor.getMessageType();
        Descriptors.FieldDescriptor wrapperValueFieldDescriptor =
            wrapperDescriptor.findFieldByNumber(1);
        DynamicMessage.Builder wrapper = DynamicMessage.newBuilder(wrapperDescriptor);
        wrapper.setField(wrapperValueFieldDescriptor, protoValue);
        return wrapper.build();
      } else {
        return protoValue;
      }
    }

    abstract @NonNull P defaultValue();

    abstract @NonNull P convertNonNullUnwrapped(@NonNull B beamValue);
  }

  abstract static class ToProtoUnwrappable<B, P> extends ToProtoField<B> {
    ToProtoUnwrappable(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public void setProtoField(Message.Builder message, B beamFieldValue) {
      @Nullable P protoValue = convert(beamFieldValue);
      if (protoValue != null) {
        Descriptors.FieldDescriptor messageFieldDescriptor =
            findDescriptor(message, fieldDescriptor);
        message.setField(messageFieldDescriptor, protoValue);
      }
    }

    @Override
    @Nullable
    public P convert(@Nullable B beamValue) {
      if (!ProtoSchemaTranslator.isNullable(fieldDescriptor) && beamValue == null) {
        return defaultValue();
      } else if (beamValue != null) {
        return convertNonNull(beamValue);
      } else {
        return null;
      }
    }

    abstract P defaultValue();

    abstract @Nullable P convertNonNull(@NonNull B beamValue);
  }

  static class ToProtoPassThrough<T> extends ToProtoWrappable<T, T> {
    private final @NonNull T defaultValue;

    public ToProtoPassThrough(
        Descriptors.FieldDescriptor fieldDescriptor, @NonNull T defaultValue) {
      super(fieldDescriptor);
      this.defaultValue = defaultValue;
    }

    @Override
    @NonNull
    T defaultValue() {
      return defaultValue;
    }

    @Override
    @NonNull
    T convertNonNullUnwrapped(@NonNull T beamValue) {
      return beamValue;
    }
  }

  static class ToProtoByteString extends ToProtoWrappable<byte[], ByteString> {
    ToProtoByteString(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    @NonNull ByteString defaultValue() {
      return ByteString.EMPTY;
    }

    @Override
    @NonNull
    ByteString convertNonNullUnwrapped(byte @NonNull [] beamValue) {
      return ByteString.copyFrom(beamValue);
    }
  }

  static class ToProtoIterableToRepeated<B>
      extends ToProtoUnwrappable<Iterable<@NonNull B>, List<@NonNull Object>> {
    private final ToProtoField<B> elementToProto;

    @SuppressWarnings("unchecked")
    ToProtoIterableToRepeated(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
      elementToProto = (ToProtoField<B>) createToProtoSingular(fieldDescriptor);
    }

    @Override
    @NonNull
    List<@NonNull Object> defaultValue() {
      return Collections.emptyList();
    }

    @Override
    @Nullable
    List<@NonNull Object> convertNonNull(@NonNull Iterable<@NonNull B> beamValue) {
      ImmutableList.Builder<@NonNull Object> builder = ImmutableList.builder();
      for (@NonNull B beamElement : beamValue) {
        Object protoElement = Preconditions.checkNotNull(elementToProto.convert(beamElement));
        builder.add(protoElement);
      }
      return builder.build();
    }
  }

  static class ToProtoMapToRepeated<BK, BV>
      extends ToProtoUnwrappable<Map<@Nullable BK, @Nullable BV>, List<@NonNull Message>> {
    private final Descriptors.Descriptor mapDescriptor;
    private final Descriptors.FieldDescriptor keyDescriptor;
    private final Descriptors.FieldDescriptor valueDescriptor;
    private final ToProtoField<BK> keyToProto;
    private final ToProtoField<BV> valueToProto;

    @SuppressWarnings("unchecked")
    ToProtoMapToRepeated(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
      mapDescriptor = fieldDescriptor.getMessageType();
      keyDescriptor = mapDescriptor.findFieldByNumber(1);
      valueDescriptor = mapDescriptor.findFieldByNumber(2);
      keyToProto = (ToProtoField<BK>) createToProtoSingular(keyDescriptor);
      valueToProto = (ToProtoField<BV>) createToProtoSingular(valueDescriptor);
    }

    @Override
    List<@NonNull Message> defaultValue() {
      return Collections.emptyList();
    }

    @Override
    @Nullable
    List<@NonNull Message> convertNonNull(@NonNull Map<@Nullable BK, @Nullable BV> beamValue) {
      ImmutableList.Builder<Message> protoList = ImmutableList.builder();
      beamValue.forEach(
          (k, v) -> {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(mapDescriptor);
            Object protoKey = Preconditions.checkNotNull(keyToProto.convert(k));
            message.setField(keyDescriptor, protoKey);
            Object protoValue = Preconditions.checkNotNull(valueToProto.convert(v));
            message.setField(valueDescriptor, protoValue);
            protoList.add(message.build());
          });
      return protoList.build();
    }
  }

  static class ToProtoEnum
      extends ToProtoUnwrappable<EnumerationType.Value, Descriptors.EnumValueDescriptor> {
    ToProtoEnum(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    Descriptors.EnumValueDescriptor defaultValue() {
      return fieldDescriptor.getEnumType().findValueByNumber(0);
    }

    @Override
    Descriptors.@NonNull EnumValueDescriptor convertNonNull(
        EnumerationType.@NonNull Value beamValue) {
      return fieldDescriptor.getEnumType().findValueByNumber(beamValue.getValue());
    }
  }

  static class ToProtoMessage extends ToProtoUnwrappable<Row, Message> {
    private final Descriptors.Descriptor descriptor;
    private final SerializableFunction<Row, Message> converter;

    ToProtoMessage(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
      this.descriptor = fieldDescriptor.getMessageType();
      this.converter = ProtoBeamConverter.toProto(descriptor);
    }

    @Override
    Message defaultValue() {
      return DynamicMessage.newBuilder(descriptor).build();
    }

    @Override
    @NonNull
    Message convertNonNull(@NonNull Row beamValue) {
      return converter.apply(beamValue);
    }
  }

  static class ToProtoOneOf implements ToProto<OneOfType.Value> {
    private final Map<Integer, ToProtoField<Object>> converters;

    ToProtoOneOf(Descriptors.OneofDescriptor oneofDescriptor) {
      this.converters = createConverters(oneofDescriptor.getFields());
    }

    @SuppressWarnings("unchecked")
    private static Map<Integer, ToProtoField<Object>> createConverters(
        List<Descriptors.FieldDescriptor> fieldDescriptors) {
      ImmutableMap.Builder<Integer, ToProtoField<Object>> builder = ImmutableMap.builder();
      for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
        builder.put(
            fieldDescriptor.getNumber(),
            (ToProtoField<Object>) createToProtoSingular(fieldDescriptor));
      }
      return builder.build();
    }

    @Override
    public void setProtoField(Message.Builder message, OneOfType.Value oneOfValue) {
      if (oneOfValue != null) {
        int number = oneOfValue.getCaseType().getValue();
        ToProtoField<Object> converter = Preconditions.checkNotNull(converters.get(number));
        converter.setProtoField(message, oneOfValue.getValue());
      }
    }
  }

  static class ToProtoDuration extends ToProtoUnwrappable<Duration, com.google.protobuf.Duration> {
    ToProtoDuration(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    com.google.protobuf.@NonNull Duration defaultValue() {
      return com.google.protobuf.Duration.getDefaultInstance();
    }

    @Override
    com.google.protobuf.@NonNull Duration convertNonNull(@NonNull Duration beamValue) {
      return com.google.protobuf.Duration.newBuilder()
          .setSeconds(beamValue.getSeconds())
          .setNanos(beamValue.getNano())
          .build();
    }
  }

  static class ToProtoTimestamp extends ToProtoUnwrappable<Instant, com.google.protobuf.Timestamp> {
    ToProtoTimestamp(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    com.google.protobuf.Timestamp defaultValue() {
      return com.google.protobuf.Timestamp.getDefaultInstance();
    }

    @Override
    @NonNull
    Timestamp convertNonNull(@NonNull Instant beamValue) {
      return com.google.protobuf.Timestamp.newBuilder()
          .setSeconds(beamValue.getEpochSecond())
          .setNanos(beamValue.getNano())
          .build();
    }
  }

  abstract static class BeamConverter<P, B> {
    protected final Schema.FieldType fieldType;

    BeamConverter(Schema.FieldType fieldType) {
      this.fieldType = fieldType;
    }

    abstract @Nullable B convert(@Nullable P protoValue);
  }

  abstract static class BeamWrappableConverter<P, U, B> extends BeamConverter<P, B> {

    BeamWrappableConverter(Schema.FieldType fieldType) {
      super(fieldType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public @Nullable B convert(@Nullable P protoValue) {
      if (fieldType.getNullable() && protoValue == null) {
        return null;
      }

      if (protoValue != null) {
        @NonNull U unwrappedProtoValue;
        if (protoValue instanceof Message) {
          // A google protobuf wrapper
          Message protoWrapper = (Message) protoValue;
          Descriptors.FieldDescriptor wrapperValueFieldDescriptor =
              protoWrapper.getDescriptorForType().findFieldByNumber(1);
          unwrappedProtoValue =
              (@NonNull U)
                  Preconditions.checkNotNull(protoWrapper.getField(wrapperValueFieldDescriptor));
        } else {
          unwrappedProtoValue = (@NonNull U) protoValue;
        }
        return convertNonNullWrapped(unwrappedProtoValue);
      } else {
        return defaultValue();
      }
    }

    protected abstract @Nullable B defaultValue();

    abstract @NonNull B convertNonNullWrapped(@NonNull U protoValue);
  }

  abstract static class BeamUnwrappableConverter<P, B> extends BeamConverter<P, B> {
    BeamUnwrappableConverter(Schema.FieldType fieldType) {
      super(fieldType);
    }

    @Override
    @Nullable
    B convert(@Nullable P protoValue) {
      if (fieldType.getNullable() && protoValue == null) {
        return null;
      }

      if (protoValue != null) {
        return convertNonNull(protoValue);
      } else {
        return defaultValue();
      }
    }

    protected abstract @NonNull B defaultValue();

    abstract @NonNull B convertNonNull(@NonNull P protoValue);
  }

  static class BeamBytesConverter extends BeamWrappableConverter<Object, ByteString, byte[]> {
    public BeamBytesConverter(Schema.FieldType fieldType) {
      super(fieldType);
    }

    @Override
    protected byte @NonNull [] defaultValue() {
      return new byte[0];
    }

    @Override
    protected byte @NonNull [] convertNonNullWrapped(@NonNull ByteString protoValue) {
      return protoValue.toByteArray();
    }
  }

  static class BeamListConverter<P, B>
      extends BeamUnwrappableConverter<List<@NonNull P>, List<@NonNull B>> {
    private final BeamConverter<P, B> elementConverter;

    @SuppressWarnings("unchecked")
    public BeamListConverter(Schema.FieldType fieldType) {
      super(fieldType);
      elementConverter =
          (BeamConverter<P, B>)
              createBeamConverter(Preconditions.checkNotNull(fieldType.getCollectionElementType()));
    }

    @Override
    protected @NonNull List<@NonNull B> defaultValue() {
      return Collections.emptyList();
    }

    @Override
    protected @NonNull List<@NonNull B> convertNonNull(@NonNull List<@NonNull P> protoList) {
      List<@NonNull B> beamList = new ArrayList<>();
      for (@NonNull P element : protoList) {
        beamList.add(Preconditions.checkNotNull(elementConverter.convert(element)));
      }
      return beamList;
    }
  }

  static class BeamMapConverter<PK, PV, BK, BV>
      extends BeamUnwrappableConverter<List<@NonNull Message>, Map<@NonNull BK, @NonNull BV>> {
    private final BeamConverter<PK, BK> keyConverter;
    private final BeamConverter<PV, BV> valueConverter;

    @SuppressWarnings("unchecked")
    public BeamMapConverter(Schema.FieldType fieldType) {
      super(fieldType);
      keyConverter =
          (BeamConverter<PK, BK>)
              createBeamConverter(Preconditions.checkNotNull(fieldType.getMapKeyType()));
      valueConverter =
          (BeamConverter<PV, BV>)
              createBeamConverter(Preconditions.checkNotNull(fieldType.getMapValueType()));
    }

    @Override
    protected @NonNull Map<@NonNull BK, @NonNull BV> defaultValue() {
      return Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected @NonNull Map<@NonNull BK, @NonNull BV> convertNonNull(
        @NonNull List<@NonNull Message> protoList) {
      if (protoList.isEmpty()) {
        return Collections.emptyMap();
      }
      Descriptors.Descriptor descriptor = protoList.get(0).getDescriptorForType();
      Descriptors.FieldDescriptor keyFieldDescriptor = descriptor.findFieldByNumber(1);
      Descriptors.FieldDescriptor valueFieldDescriptor = descriptor.findFieldByNumber(2);
      Map<@NonNull BK, @NonNull BV> beamMap = new HashMap<>();
      protoList.forEach(
          protoElement -> {
            PK protoKey =
                Preconditions.checkNotNull((PK) protoElement.getField(keyFieldDescriptor));
            PV protoValue =
                Preconditions.checkNotNull((PV) protoElement.getField(valueFieldDescriptor));
            BK beamKey = Preconditions.checkNotNull(keyConverter.convert(protoKey));
            BV beamValue = Preconditions.checkNotNull(valueConverter.convert(protoValue));
            beamMap.put(beamKey, beamValue);
          });
      return beamMap;
    }
  }

  static class BeamRowConverter extends BeamUnwrappableConverter<Message, Row> {
    private final Schema rowSchema;
    private final SerializableFunction<@NonNull Message, @NonNull Row> converter;

    public BeamRowConverter(Schema.FieldType fieldType) {
      super(fieldType);
      rowSchema = Preconditions.checkNotNull(fieldType.getRowSchema());
      converter = ProtoBeamConverter.fromProto(rowSchema);
    }

    @Override
    protected @NonNull Row defaultValue() {
      return Row.withSchema(rowSchema).build();
    }

    @Override
    protected @NonNull Row convertNonNull(@NonNull Message protoValue) {
      return converter.apply(protoValue);
    }
  }

  static class BeamPassThroughConverter<T> extends BeamWrappableConverter<Object, T, T> {
    private final @NonNull T defaultValue;

    public BeamPassThroughConverter(Schema.FieldType fieldType, @NonNull T defaultValue) {
      super(fieldType);
      this.defaultValue = defaultValue;
    }

    @Override
    protected @NonNull T defaultValue() {
      return defaultValue;
    }

    @Override
    protected @NonNull T convertNonNullWrapped(@NotNull T protoValue) {
      return protoValue;
    }
  }

  static class BeamNanosDurationConverter extends BeamUnwrappableConverter<Message, Duration> {
    BeamNanosDurationConverter(Schema.FieldType fieldType) {
      super(fieldType);
    }

    @Override
    protected @NonNull Duration defaultValue() {
      return Duration.ZERO;
    }

    @Override
    protected @NonNull Duration convertNonNull(@NonNull Message protoValue) {
      Descriptors.Descriptor durationDescriptor = protoValue.getDescriptorForType();
      Descriptors.FieldDescriptor secondsFieldDescriptor = durationDescriptor.findFieldByNumber(1);
      Descriptors.FieldDescriptor nanosFieldDescriptor = durationDescriptor.findFieldByNumber(2);
      long seconds = (long) protoValue.getField(secondsFieldDescriptor);
      int nanos = (int) protoValue.getField(nanosFieldDescriptor);
      return Duration.ofSeconds(seconds, nanos);
    }
  }

  static class BeamNanosInstantConverter extends BeamUnwrappableConverter<Message, Instant> {
    BeamNanosInstantConverter(Schema.FieldType fieldType) {
      super(fieldType);
    }

    @Override
    protected @NonNull Instant defaultValue() {
      return Instant.EPOCH;
    }

    @Override
    protected @NonNull Instant convertNonNull(@NonNull Message protoValue) {
      Descriptors.Descriptor timestampDescriptor = protoValue.getDescriptorForType();
      Descriptors.FieldDescriptor secondsFieldDescriptor = timestampDescriptor.findFieldByNumber(1);
      Descriptors.FieldDescriptor nanosFieldDescriptor = timestampDescriptor.findFieldByNumber(2);
      long seconds = (long) protoValue.getField(secondsFieldDescriptor);
      int nanos = (int) protoValue.getField(nanosFieldDescriptor);
      return Instant.ofEpochSecond(seconds, nanos);
    }
  }

  static class BeamEnumerationConverter
      extends BeamUnwrappableConverter<Descriptors.EnumValueDescriptor, EnumerationType.Value> {
    private final EnumerationType enumerationType;

    BeamEnumerationConverter(Schema.FieldType fieldType) {
      super(fieldType);
      enumerationType = fieldType.getLogicalType(EnumerationType.class);
    }

    @Override
    protected EnumerationType.@NonNull Value defaultValue() {
      return enumerationType.toInputType(0);
    }

    @Override
    protected EnumerationType.@NonNull Value convertNonNull(
        Descriptors.@NonNull EnumValueDescriptor protoValue) {
      int number = protoValue.getNumber();
      return enumerationType.toInputType(number);
    }
  }

  static class FromProtoOneOf implements FromProto<OneOfType.Value> {
    private final OneOfType oneOfType;
    private final Map<String, BeamConverter<Object, ?>> converter;

    FromProtoOneOf(Schema.FieldType fieldType) {
      this.oneOfType = Preconditions.checkNotNull(fieldType.getLogicalType(OneOfType.class));
      this.converter = createConverters(oneOfType.getOneOfSchema());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, BeamConverter<Object, ?>> createConverters(Schema schema) {
      Map<String, BeamConverter<Object, ?>> converters = new HashMap<>();
      for (Schema.Field field : schema.getFields()) {
        converters.put(
            field.getName(), (BeamConverter<Object, ?>) createBeamConverter(field.getType()));
      }
      return converters;
    }

    @Override
    public OneOfType.@Nullable Value getBeamField(Message message) {
      Descriptors.Descriptor descriptor = message.getDescriptorForType();
      for (Map.Entry<String, BeamConverter<Object, ?>> entry : converter.entrySet()) {
        String fieldName = entry.getKey();
        BeamConverter<Object, ?> value = entry.getValue();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
        if (message.hasField(fieldDescriptor)) {
          Object protoValue = Preconditions.checkNotNull(message.getField(fieldDescriptor));
          return oneOfType.createValue(fieldName, value.convert(protoValue));
        }
      }
      return null;
    }
  }
}
