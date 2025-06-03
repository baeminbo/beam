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
    Map<String, ToProtoSetter<?>> toProtos = new LinkedHashMap<>();
    for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
      if (fieldDescriptor.getRealContainingOneof() != null) {
        Descriptors.OneofDescriptor realContainingOneof = fieldDescriptor.getRealContainingOneof();
        if (realContainingOneof.getField(0) == fieldDescriptor) {
          toProtos.put(realContainingOneof.getName(), new ToProtoOneOfSetter(realContainingOneof));
        }
        // continue
      } else {
        toProtos.put(fieldDescriptor.getName(), createToProtoFieldSetter(fieldDescriptor));
      }
    }
    return row -> {
      DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor);
      for (Map.Entry<String, ToProtoSetter<?>> entry : toProtos.entrySet()) {
        String fieldName = entry.getKey();
        @SuppressWarnings("unchecked")
        ToProtoSetter<Object> converter = (ToProtoSetter<Object>) entry.getValue();
        converter.setToProto(message, row.getValue(fieldName));
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

  static ToProtoFieldSetter<?> createToProtoFieldSetter(
      Descriptors.FieldDescriptor fieldDescriptor) {
    if (fieldDescriptor.isRepeated()) {
      if (fieldDescriptor.isMapField()) {
        return new ToProtoMapSetter<>(fieldDescriptor);
      } else {
        return new ToProtoListSetter<>(fieldDescriptor);
      }
    } else {
      return createToProtoSingularSetter(fieldDescriptor);
    }
  }

  static ToProtoFieldSetter<?> createToProtoSingularSetter(
      Descriptors.FieldDescriptor fieldDescriptor) {
    switch (fieldDescriptor.getJavaType()) {
      case INT:
        return new ToProtoPassThroughSetter<>(fieldDescriptor, 0);
      case LONG:
        return new ToProtoPassThroughSetter<>(fieldDescriptor, 0L);
      case FLOAT:
        return new ToProtoPassThroughSetter<>(fieldDescriptor, 0f);
      case DOUBLE:
        return new ToProtoPassThroughSetter<>(fieldDescriptor, 0.0);
      case BOOLEAN:
        return new ToProtoPassThroughSetter<>(fieldDescriptor, false);
      case STRING:
        return new ToProtoPassThroughSetter<>(fieldDescriptor, "");
      case BYTE_STRING:
        return new ToProtoByteStringSetter(fieldDescriptor);
      case ENUM:
        return new ToProtoEnumSetter(fieldDescriptor);
      case MESSAGE:
        String fullName = fieldDescriptor.getMessageType().getFullName();
        switch (fullName) {
          case "google.protobuf.Int32Value":
          case "google.protobuf.UInt32Value":
            return new ToProtoPassThroughSetter<>(fieldDescriptor, 0);
          case "google.protobuf.Int64Value":
          case "google.protobuf.UInt64Value":
            return new ToProtoPassThroughSetter<>(fieldDescriptor, 0L);
          case "google.protobuf.FloatValue":
            return new ToProtoPassThroughSetter<>(fieldDescriptor, 0f);
          case "google.protobuf.DoubleValue":
            return new ToProtoPassThroughSetter<>(fieldDescriptor, 0.0);
          case "google.protobuf.StringValue":
            return new ToProtoPassThroughSetter<>(fieldDescriptor, "");
          case "google.protobuf.BoolValue":
            return new ToProtoPassThroughSetter<>(fieldDescriptor, false);
          case "google.protobuf.BytesValue":
            return new ToProtoByteStringSetter(fieldDescriptor);
          case "google.protobuf.Timestamp":
            return new ToProtoTimestampSetter(fieldDescriptor);
          case "google.protobuf.Duration":
            return new ToProtoDurationSetter(fieldDescriptor);
          case "google.protobuf.Any":
            throw new UnsupportedOperationException("google.protobuf.Any is not supported");
          default:
            return new ToProtoMessageSetter(fieldDescriptor);
        }
      default:
        throw new UnsupportedOperationException(
            "Unsupported proto type: " + fieldDescriptor.getJavaType());
    }
  }

  public static SerializableFunction<@NonNull Message, @NonNull Row> fromProto(Schema schema) {
    List<FromProtoGetter<?>> toBeams = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      Schema.FieldType fieldType = field.getType();
      if (fieldType.isLogicalType(OneOfType.IDENTIFIER)) {
        toBeams.add(new FromProtoOneOfGetter(fieldType));
      } else {
        toBeams.add(new FromProtoFieldGetter<>(field));
      }
    }

    return message -> {
      Row.Builder rowBuilder = Row.withSchema(schema);
      for (FromProtoGetter<?> toBeam : toBeams) {
        rowBuilder.addValue(toBeam.getFromProto(message));
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

  interface ToProtoSetter<B> {
    void setToProto(Message.Builder message, B beamFieldValue);
  }

  interface FromProtoGetter<B> {
    @Nullable
    B getFromProto(Message message);
  }

  static class FromProtoFieldGetter<P, B> implements FromProtoGetter<B> {
    private final Schema.Field field;
    private final BeamConverter<P, B> converter;

    @SuppressWarnings("unchecked")
    FromProtoFieldGetter(Schema.Field field) {
      this.field = field;
      converter = (BeamConverter<P, B>) createBeamConverter(field.getType());
    }

    @SuppressWarnings("unchecked")
    @Override
    public @Nullable B getFromProto(Message message) {
      Descriptors.Descriptor descriptor = message.getDescriptorForType();
      Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(field.getName());
      @Nullable Object protoValue = message.getField(fieldDescriptor);
      return converter.convert((P) protoValue);
    }
  }

  abstract static class ToProtoFieldSetter<B> implements ToProtoSetter<B> {
    protected final Descriptors.FieldDescriptor fieldDescriptor;

    ToProtoFieldSetter(Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
    }

    public abstract @Nullable Object convert(@Nullable B beamValue);
  }

  abstract static class ToProtoWrappableSetter<B, P> extends ToProtoFieldSetter<B> {
    ToProtoWrappableSetter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public void setToProto(Message.Builder message, B beamFieldValue) {
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

  abstract static class ToProtoUnwrappableSetter<B, P> extends ToProtoFieldSetter<B> {
    ToProtoUnwrappableSetter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public void setToProto(Message.Builder message, B beamFieldValue) {
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

  static class ToProtoPassThroughSetter<T> extends ToProtoWrappableSetter<T, T> {
    private final @NonNull T defaultValue;

    public ToProtoPassThroughSetter(
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

  static class ToProtoByteStringSetter extends ToProtoWrappableSetter<byte[], ByteString> {
    ToProtoByteStringSetter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    @NonNull
    ByteString defaultValue() {
      return ByteString.EMPTY;
    }

    @Override
    @NonNull
    ByteString convertNonNullUnwrapped(byte @NonNull [] beamValue) {
      return ByteString.copyFrom(beamValue);
    }
  }

  static class ToProtoListSetter<B>
      extends ToProtoUnwrappableSetter<Iterable<@NonNull B>, List<@NonNull Object>> {
    private final ToProtoFieldSetter<B> elementToProto;

    @SuppressWarnings("unchecked")
    ToProtoListSetter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
      elementToProto = (ToProtoFieldSetter<B>) createToProtoSingularSetter(fieldDescriptor);
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

  static class ToProtoMapSetter<BK, BV>
      extends ToProtoUnwrappableSetter<Map<@Nullable BK, @Nullable BV>, List<@NonNull Message>> {
    private final Descriptors.Descriptor mapDescriptor;
    private final Descriptors.FieldDescriptor keyDescriptor;
    private final Descriptors.FieldDescriptor valueDescriptor;
    private final ToProtoFieldSetter<BK> keyToProto;
    private final ToProtoFieldSetter<BV> valueToProto;

    @SuppressWarnings("unchecked")
    ToProtoMapSetter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
      mapDescriptor = fieldDescriptor.getMessageType();
      keyDescriptor = mapDescriptor.findFieldByNumber(1);
      valueDescriptor = mapDescriptor.findFieldByNumber(2);
      keyToProto = (ToProtoFieldSetter<BK>) createToProtoSingularSetter(keyDescriptor);
      valueToProto = (ToProtoFieldSetter<BV>) createToProtoSingularSetter(valueDescriptor);
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

  static class ToProtoEnumSetter
      extends ToProtoUnwrappableSetter<EnumerationType.Value, Descriptors.EnumValueDescriptor> {
    ToProtoEnumSetter(Descriptors.FieldDescriptor fieldDescriptor) {
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

  static class ToProtoMessageSetter extends ToProtoUnwrappableSetter<Row, Message> {
    private final Descriptors.Descriptor descriptor;
    private final SerializableFunction<Row, Message> converter;

    ToProtoMessageSetter(Descriptors.FieldDescriptor fieldDescriptor) {
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

  static class ToProtoOneOfSetter implements ToProtoSetter<OneOfType.Value> {
    private final Map<Integer, ToProtoFieldSetter<Object>> converters;

    ToProtoOneOfSetter(Descriptors.OneofDescriptor oneofDescriptor) {
      this.converters = createConverters(oneofDescriptor.getFields());
    }

    @SuppressWarnings("unchecked")
    private static Map<Integer, ToProtoFieldSetter<Object>> createConverters(
        List<Descriptors.FieldDescriptor> fieldDescriptors) {
      ImmutableMap.Builder<Integer, ToProtoFieldSetter<Object>> builder = ImmutableMap.builder();
      for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
        builder.put(
            fieldDescriptor.getNumber(),
            (ToProtoFieldSetter<Object>) createToProtoSingularSetter(fieldDescriptor));
      }
      return builder.build();
    }

    @Override
    public void setToProto(Message.Builder message, OneOfType.Value oneOfValue) {
      if (oneOfValue != null) {
        int number = oneOfValue.getCaseType().getValue();
        ToProtoFieldSetter<Object> converter = Preconditions.checkNotNull(converters.get(number));
        converter.setToProto(message, oneOfValue.getValue());
      }
    }
  }

  static class ToProtoDurationSetter
      extends ToProtoUnwrappableSetter<Duration, com.google.protobuf.Duration> {
    ToProtoDurationSetter(Descriptors.FieldDescriptor fieldDescriptor) {
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

  static class ToProtoTimestampSetter extends ToProtoUnwrappableSetter<Instant, Timestamp> {
    ToProtoTimestampSetter(Descriptors.FieldDescriptor fieldDescriptor) {
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

  static class FromProtoOneOfGetter implements FromProtoGetter<OneOfType.Value> {
    private final OneOfType oneOfType;
    private final Map<String, BeamConverter<Object, ?>> converter;

    FromProtoOneOfGetter(Schema.FieldType fieldType) {
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
    public OneOfType.@Nullable Value getFromProto(Message message) {
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
