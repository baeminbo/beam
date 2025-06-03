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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

public class ProtoDynamicMessageConverter {

  public static SerializableFunction<@NonNull Row, @NonNull Message> toProto(
      Descriptors.Descriptor descriptor) {
    List<ToProto> toProtos = new ArrayList<>();
    for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
      if (fieldDescriptor.getRealContainingOneof() != null) {
        Descriptors.OneofDescriptor realContainingOneof = fieldDescriptor.getRealContainingOneof();
        if (realContainingOneof.getField(0) == fieldDescriptor) {
          toProtos.add(new ToProtoOneOf(realContainingOneof));
        } else {
          // skip non-first field for a real oneof.
          continue;
        }
      } else if (fieldDescriptor.isRepeated()) {
        if (fieldDescriptor.isMapField()) {
          toProtos.add(new ToProtoMapToRepeated<>(fieldDescriptor));
        } else {
          toProtos.add(new ToProtoIterableToRepeated<>(fieldDescriptor));
        }
      } else {
        toProtos.add(createToProtoSingular(fieldDescriptor));
      }
    }
    return row -> {
      DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor);
      for (ToProto toProto : toProtos) {
        toProto.setProtoField(message, row);
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
        return new ToProtoMessage(fieldDescriptor);
      default:
        throw new UnsupportedOperationException(
            "Unsupported proto type: " + fieldDescriptor.getJavaType());
    }
  }

  public static SerializableFunction<@NonNull Message, @NonNull Row> toBeam(Schema schema) {
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

  static BeamConverter<?> createBeamConverter(Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case BYTE:
        throw new UnsupportedOperationException();
      case INT16:
        throw new UnsupportedOperationException();
      case INT32:
        return new BeamPassThroughType<>(fieldType, 0);
      case INT64:
        return new BeamPassThroughType<>(fieldType, 0L);
      case DECIMAL:
        throw new UnsupportedOperationException();
      case FLOAT:
        return new BeamPassThroughType<>(fieldType, 0f);
      case DOUBLE:
        return new BeamPassThroughType<>(fieldType, 0.0);
      case STRING:
        return new BeamPassThroughType<>(fieldType, "");
      case DATETIME:
        throw new UnsupportedOperationException();
      case BOOLEAN:
        return new BeamPassThroughType<>(fieldType, false);
      case BYTES:
        return new BeamBytesConverter(fieldType);
      case ARRAY:
      case ITERABLE:
        return new BeamListConverter<>(fieldType);
      case MAP:
        return new BeamMapConverter<>(fieldType);
      case ROW:
        return new BeamRow(fieldType);
      case LOGICAL_TYPE:
        switch (Preconditions.checkNotNull(fieldType.getLogicalType()).getIdentifier()) {
          case ProtoSchemaLogicalTypes.UInt32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SInt32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.Fixed32.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SFixed32.IDENTIFIER:
            return new BeamPassThroughType<>(fieldType, 0);
          case ProtoSchemaLogicalTypes.UInt64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SInt64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.Fixed64.IDENTIFIER:
          case ProtoSchemaLogicalTypes.SFixed64.IDENTIFIER:
            return new BeamPassThroughType<>(fieldType, 0L);
          case NanosDuration.IDENTIFIER:
            return new BeamNanosDurationConverter(fieldType);
          case NanosInstant.IDENTIFIER:
            return new BeamNanosInstant(fieldType);
          case EnumerationType.IDENTIFIER:
            return new BeamEnumeration(fieldType);
          default:
            throw new UnsupportedOperationException();
        }
      default:
        throw new UnsupportedOperationException();
    }
  }

  interface ToProto {
    void setProtoField(Message.Builder message, Row row);
  }

  interface FromProto<B> {
    @Nullable
    B getBeamField(Message message);
  }

  static class FromProtoField<B> implements FromProto<B> {
    private final Schema.Field field;
    private final BeamConverter<B> converter;

    @SuppressWarnings("unchecked")
    FromProtoField(Schema.Field field) {
      this.field = field;
      converter = (BeamConverter<B>) createBeamConverter(field.getType());
    }

    @Override
    public @Nullable B getBeamField(Message message) {
      Descriptors.Descriptor descriptor = message.getDescriptorForType();
      Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(field.getName());
      @Nullable Object protoValue = message.getField(fieldDescriptor);
      return converter.convert(protoValue);
    }
  }

  abstract static class ToProtoField<B> implements ToProto {
    protected final Descriptors.FieldDescriptor fieldDescriptor;

    ToProtoField(Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
    }

    public abstract @Nullable Object convert(B beamValue);
  }

  abstract static class ToProtoWrappable<B, P> extends ToProtoField<B> {
    ToProtoWrappable(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public void setProtoField(Message.Builder message, Row row) {
      Object protoValue = convert(row.getValue(fieldDescriptor.getName()));
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

      P protoValue = beamValue != null ? convertNonNull(beamValue) : defaultValue();
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

    abstract @NonNull P convertNonNull(@NonNull B beamValue);
  }

  abstract static class ToProtoUnwrappable<B, P> extends ToProtoField<B> {
    ToProtoUnwrappable(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public void setProtoField(Message.Builder message, Row row) {
      @Nullable P protoValue = convert(row.getValue(fieldDescriptor.getName()));
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
    T convertNonNull(@NonNull T beamValue) {
      return beamValue;
    }
  }

  static class ToProtoInt extends ToProtoWrappable<Integer, Integer> {
    ToProtoInt(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    @NonNull
    Integer defaultValue() {
      return 0;
    }

    @Override
    @NonNull
    Integer convertNonNull(@NonNull Integer beamValue) {
      return beamValue;
    }
  }

  //  static class ToProtoLong extends ToProtoWrappable<Long, Long> {
  //    ToProtoLong(Descriptors.FieldDescriptor fieldDescriptor) {
  //      super(fieldDescriptor);
  //    }
  //
  //    @Override
  //    @NonNull
  //    Long defaultValue() {
  //      return 0L;
  //    }
  //
  //    @Override
  //    @NonNull
  //    Long convertNonNull(@NonNull Long beamValue) {
  //      return beamValue;
  //    }
  //  }

  //  static class ToProtoFloat extends ToProtoWrappable<Float, Float> {
  //    ToProtoFloat(Descriptors.FieldDescriptor fieldDescriptor) {
  //      super(fieldDescriptor);
  //    }
  //
  //    @Override
  //    @NonNull
  //    Float defaultValue() {
  //      return 0f;
  //    }
  //
  //    @Override
  //    @NonNull
  //    Float convertNonNull(@NonNull Float beamValue) {
  //      return beamValue;
  //    }
  //  }

  //  static class ToProtoDouble extends ToProtoWrappable<Double, Double> {
  //    ToProtoDouble(Descriptors.FieldDescriptor fieldDescriptor) {
  //      super(fieldDescriptor);
  //    }
  //
  //    @Override
  //    @NonNull
  //    Double defaultValue() {
  //      return 0.0;
  //    }
  //
  //    @Override
  //    @NonNull
  //    Double convertNonNull(@NonNull Double beamValue) {
  //      return beamValue;
  //    }
  //  }

  //  static class ToProtoBoolean extends ToProtoWrappable<Boolean, Boolean> {
  //    ToProtoBoolean(Descriptors.FieldDescriptor fieldDescriptor) {
  //      super(fieldDescriptor);
  //    }
  //
  //    @Override
  //    @NonNull
  //    Boolean defaultValue() {
  //      return false;
  //    }
  //
  //    @Override
  //    @NonNull
  //    Boolean convertNonNull(@NonNull Boolean beamValue) {
  //      return beamValue;
  //    }
  //  }
  //
  //  static class ToProtoString extends ToProtoWrappable<String, String> {
  //    ToProtoString(Descriptors.FieldDescriptor fieldDescriptor) {
  //      super(fieldDescriptor);
  //    }
  //
  //    @Override
  //    @NonNull
  //    String defaultValue() {
  //      return "";
  //    }
  //
  //    @Override
  //    @NonNull
  //    String convertNonNull(@NonNull String beamValue) {
  //      return beamValue;
  //    }
  //  }

  static class ToProtoByteString extends ToProtoUnwrappable<byte[], ByteString> {
    ToProtoByteString(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    ByteString defaultValue() {
      return ByteString.EMPTY;
    }

    @Override
    @NonNull
    ByteString convertNonNull(byte @NonNull [] beamValue) {
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

  static class ToProtoMapToRepeated<BK, BV, PK, PV>
      extends ToProtoUnwrappable<Map<@NonNull BK, @NonNull BV>, List<@NonNull Message>> {
    private final Descriptors.Descriptor mapDescriptor;
    private final Descriptors.FieldDescriptor keyDescriptor;
    private final Descriptors.FieldDescriptor valueDescriptor;
    private final ToProtoUnwrappable<BK, PK> keyToProto;
    private final ToProtoUnwrappable<BV, PV> valueToProto;

    @SuppressWarnings("unchecked")
    ToProtoMapToRepeated(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
      mapDescriptor = fieldDescriptor.getMessageType();
      keyDescriptor = mapDescriptor.findFieldByNumber(1);
      valueDescriptor = mapDescriptor.findFieldByNumber(2);
      keyToProto = (ToProtoUnwrappable<BK, PK>) createToProtoSingular(keyDescriptor);
      valueToProto = (ToProtoUnwrappable<BV, PV>) createToProtoSingular(valueDescriptor);
    }

    @Override
    List<@NonNull Message> defaultValue() {
      return Collections.emptyList();
    }

    @Override
    @Nullable
    List<@NonNull Message> convertNonNull(@NonNull Map<@NonNull BK, @NonNull BV> beamValue) {
      ImmutableList.Builder<Message> protoList = ImmutableList.builder();
      beamValue.forEach(
          (k, v) -> {
            DynamicMessage.Builder message = DynamicMessage.newBuilder(mapDescriptor);
            PK protoKey = Preconditions.checkNotNull(keyToProto.convert(k));
            message.setField(keyDescriptor, protoKey);
            PV protoValue = Preconditions.checkNotNull(valueToProto.convert(v));
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
      this.converter = ProtoDynamicMessageConverter.toProto(descriptor);
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

  static class ToProtoOneOf implements ToProto {
    private final Descriptors.OneofDescriptor oneofDescriptor;
    private final Map<Integer, ToProtoField<?>> converters;

    ToProtoOneOf(Descriptors.OneofDescriptor oneofDescriptor) {
      this.oneofDescriptor = oneofDescriptor;
      this.converters = createConverters(oneofDescriptor.getFields());
    }

    private static Map<Integer, ToProtoField<?>> createConverters(
        List<Descriptors.FieldDescriptor> fieldDescriptors) {
      ImmutableMap.Builder<Integer, ToProtoField<?>> builder = ImmutableMap.builder();
      for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
        builder.put(fieldDescriptor.getNumber(), createToProtoSingular(fieldDescriptor));
      }
      return builder.build();
    }

    @Override
    public void setProtoField(Message.Builder message, Row row) {
      OneOfType.@Nullable Value oneOfValue = row.getValue(oneofDescriptor.getName());
      if (oneOfValue != null) {
        int number = oneOfValue.getCaseType().getValue();
        ToProtoField<?> converter = Preconditions.checkNotNull(converters.get(number));
        converter.setProtoField(message, row);
      }
    }
  }

  abstract static class BeamConverter<B> {
    protected final Schema.FieldType fieldType;

    BeamConverter(Schema.FieldType fieldType) {
      this.fieldType = fieldType;
    }

    abstract @Nullable B convert(Object protoValue);
  }

  abstract static class BeamWrappableConverter<P, B> extends BeamConverter<B> {

    BeamWrappableConverter(Schema.FieldType fieldType) {
      super(fieldType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public @Nullable B convert(Object protoValue) {
      if (fieldType.getNullable() && protoValue == null) {
        return null;
      }

      if (protoValue != null) {
        if (protoValue instanceof Message) {
          // A google protobuf wrapper
          Message protoWrapper = (Message) protoValue;
          Descriptors.FieldDescriptor wrapperValueFieldDescriptor =
              protoWrapper.getDescriptorForType().findFieldByNumber(1);
          protoValue = protoWrapper.getField(wrapperValueFieldDescriptor);
        }
        return convertNonNull((@NonNull P) protoValue);
      } else {
        return defaultValue();
      }
    }

    protected abstract @Nullable B defaultValue();

    protected abstract @NonNull B convertNonNull(@NonNull P protoValue);
  }

  abstract static class BeamUnwrappableConverter<P, B> extends BeamConverter<B> {
    BeamUnwrappableConverter(Schema.FieldType fieldType) {
      super(fieldType);
    }

    @SuppressWarnings("unchecked")
    @Override
    @Nullable
    B convert(Object protoValue) {
      if (fieldType.getNullable() && protoValue == null) {
        return null;
      }

      if (protoValue != null) {
        return convertNonNull((@NonNull P) protoValue);
      } else {
        return defaultValue();
      }
    }

    protected abstract @NonNull B defaultValue();

    protected abstract @NonNull B convertNonNull(@NonNull P protoValue);
  }

  //  static class ToBeamInteger extends ToBeamWrappable<Integer, Integer> {
  //    ToBeamInteger(Schema.FieldType fieldType) {
  //      super(fieldType);
  //    }
  //
  //    @Override
  //    protected @NonNull Integer defaultValue() {
  //      return 0;
  //    }
  //
  //    @Override
  //    protected @NonNull Integer convertNonNull(@NonNull Integer protoValue) {
  //      return protoValue;
  //    }
  //  }
  //
  //  static class ToBeamLong extends ToBeamWrappable<Long, Long> {
  //    ToBeamLong(Schema.FieldType fieldType) {
  //      super(fieldType);
  //    }
  //
  //    @Override
  //    protected @NonNull Long defaultValue() {
  //      return 0L;
  //    }
  //
  //    @Override
  //    protected @NonNull Long convertNonNull(@NonNull Long protoValue) {
  //      return protoValue;
  //    }
  //  }
  //
  //  static class ToBeamFloat extends ToBeamWrappable<Float, Float> {
  //    ToBeamFloat(Schema.FieldType fieldType) {
  //      super(fieldType);
  //    }
  //
  //    @Override
  //    protected @NonNull Float defaultValue() {
  //      return 0f;
  //    }
  //
  //    @Override
  //    protected @NonNull Float convertNonNull(@NonNull Float protoValue) {
  //      return protoValue;
  //    }
  //  }
  //
  //  static class ToBeamDouble extends ToBeamWrappable<Double, Double> {
  //    ToBeamDouble(Schema.FieldType fieldType) {
  //      super(fieldType);
  //    }
  //
  //    @Override
  //    protected @NonNull Double defaultValue() {
  //      return 0.0;
  //    }
  //
  //    @Override
  //    protected @NonNull Double convertNonNull(@NonNull Double protoValue) {
  //      return protoValue;
  //    }
  //  }
  //
  //  static class ToBeamString extends ToBeamWrappable<String, String> {
  //    ToBeamString(Schema.FieldType fieldType) {
  //      super(fieldType);
  //    }
  //
  //    @Override
  //    protected @NonNull String defaultValue() {
  //      return "";
  //    }
  //
  //    @Override
  //    protected @NonNull String convertNonNull(@NonNull String protoValue) {
  //      return protoValue;
  //    }
  //  }

  //  static class ToBeamBoolean extends ToBeamWrappable<Boolean, Boolean> {
  //    ToBeamBoolean(Schema.FieldType fieldType) {
  //      super(fieldType);
  //    }
  //
  //    @Override
  //    protected @NonNull Boolean defaultValue() {
  //      return false;
  //    }
  //
  //    @Override
  //    protected @NonNull Boolean convertNonNull(@NonNull Boolean protoValue) {
  //      return protoValue;
  //    }
  //  }

  static class BeamBytesConverter extends BeamWrappableConverter<ByteString, byte[]> {
    public BeamBytesConverter(Schema.FieldType fieldType) {
      super(fieldType);
    }

    @Override
    protected byte @NonNull [] defaultValue() {
      return new byte[0];
    }

    @Override
    protected byte @NonNull [] convertNonNull(@NonNull ByteString protoValue) {
      return protoValue.toByteArray();
    }
  }

  static class BeamListConverter<P, B>
      extends BeamUnwrappableConverter<List<@NonNull P>, List<B>> {
    private final BeamWrappableConverter<P, B> elementConverter;

    @SuppressWarnings("unchecked")
    public BeamListConverter(Schema.FieldType fieldType) {
      super(fieldType);
      elementConverter =
          (BeamWrappableConverter<P, B>)
              createBeamConverter(Preconditions.checkNotNull(fieldType.getCollectionElementType()));
    }

    @Override
    protected @NonNull List<B> defaultValue() {
      return Collections.emptyList();
    }

    @Override
    protected @NonNull List<B> convertNonNull(@NonNull List<@NonNull P> protoList) {
      List<B> beamList = new ArrayList<>();
      for (P element : protoList) {
        beamList.add(elementConverter.convertNonNull(element));
      }
      return beamList;
    }
  }

  static class BeamMapConverter<PK, PV, BK, BV>
      extends BeamUnwrappableConverter<List<Message>, Map<BK, BV>> {
    private final BeamWrappableConverter<PK, BK> keyConverter;
    private final BeamWrappableConverter<PV, BV> valueConverter;

    @SuppressWarnings("unchecked")
    public BeamMapConverter(Schema.FieldType fieldType) {
      super(fieldType);
      keyConverter =
          (BeamWrappableConverter<PK, BK>)
              createBeamConverter(Preconditions.checkNotNull(fieldType.getMapKeyType()));
      valueConverter =
          (BeamWrappableConverter<PV, BV>)
              createBeamConverter(Preconditions.checkNotNull(fieldType.getMapValueType()));
    }

    @Override
    protected @NonNull Map<BK, BV> defaultValue() {
      return Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected @NonNull Map<BK, BV> convertNonNull(@NonNull List<Message> protoList) {
      if (protoList.isEmpty()) {
        return Collections.emptyMap();
      }
      Descriptors.Descriptor descriptor = protoList.get(0).getDescriptorForType();
      Descriptors.FieldDescriptor keyFieldDescriptor = descriptor.findFieldByNumber(1);
      Descriptors.FieldDescriptor valueFieldDescriptor = descriptor.findFieldByNumber(2);
      Map<BK, BV> beamMap = new HashMap<>();
      protoList.forEach(
          protoElement -> {
            PK protoKey =
                Preconditions.checkNotNull((PK) protoElement.getField(keyFieldDescriptor));
            PV protoValue =
                Preconditions.checkNotNull((PV) protoElement.getField(valueFieldDescriptor));
            BK beamKey = keyConverter.convertNonNull(protoKey);
            BV beamValue = valueConverter.convertNonNull(protoValue);
            beamMap.put(beamKey, beamValue);
          });
      return beamMap;
    }
  }

  private static class BeamRow extends BeamUnwrappableConverter<Message, Row> {
    private final Schema rowSchema;
    private final SerializableFunction<@NonNull Message, @NonNull Row> converter;

    public BeamRow(Schema.FieldType fieldType) {
      super(fieldType);
      rowSchema = Preconditions.checkNotNull(fieldType.getRowSchema());
      converter = ProtoDynamicMessageConverter.toBeam(rowSchema);
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

  private static class BeamPassThroughType<T> extends BeamWrappableConverter<T, T> {
    private final @NonNull T defaultValue;

    public BeamPassThroughType(Schema.FieldType fieldType, @NonNull T defaultValue) {
      super(fieldType);
      this.defaultValue = defaultValue;
    }

    @Override
    protected @NonNull T defaultValue() {
      return defaultValue;
    }

    @Override
    protected @NonNull T convertNonNull(@NotNull T protoValue) {
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
      int seconds = (int) protoValue.getField(secondsFieldDescriptor);
      long nanos = (long) protoValue.getField(nanosFieldDescriptor);
      return Duration.ofSeconds(seconds, nanos);
    }
  }

  private static class BeamNanosInstant extends BeamUnwrappableConverter<Message, Instant> {
    BeamNanosInstant(Schema.FieldType fieldType) {
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
      int seconds = (int) protoValue.getField(secondsFieldDescriptor);
      long nanos = (long) protoValue.getField(nanosFieldDescriptor);
      return Instant.ofEpochSecond(seconds, nanos);
    }
  }

  private static class BeamEnumeration
      extends BeamUnwrappableConverter<Descriptors.EnumValueDescriptor, EnumerationType.Value> {
    private final EnumerationType enumerationType;

    BeamEnumeration(Schema.FieldType fieldType) {
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
    private final Map<String, BeamConverter<?>> converter;

    FromProtoOneOf(Schema.FieldType fieldType) {
      this.oneOfType = Preconditions.checkNotNull(fieldType.getLogicalType(OneOfType.class));
      this.converter = createConverters(oneOfType.getOneOfSchema());
    }

    private static Map<String, BeamConverter<?>> createConverters(Schema schema) {
      Map<String, BeamConverter<?>> converters = new HashMap<>();
      for (Schema.Field field : schema.getFields()) {
        converters.put(field.getName(), createBeamConverter(field.getType()));
      }
      return converters;
    }

    @Override
    public OneOfType.@Nullable Value getBeamField(Message message) {
      Descriptors.Descriptor descriptor = message.getDescriptorForType();
      for (Map.Entry<String, BeamConverter<?>> entry : converter.entrySet()) {
        String fieldName = entry.getKey();
        BeamConverter<?> value = entry.getValue();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
        Object protoValue = message.getField(fieldDescriptor);
        if (protoValue != null) {
          return oneOfType.createValue(fieldName, value.convert(protoValue));
        }
      }
      return null;
    }
  }
}
