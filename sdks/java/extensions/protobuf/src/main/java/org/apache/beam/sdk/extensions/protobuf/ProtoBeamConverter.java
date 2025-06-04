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
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
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

/**
 * Provides converts between Protobuf Message and Beam Row.
 *
 * <p>Read <a href="https://s.apache.org/beam-protobuf">https://s.apache.org/beam-protobuf</a>
 */
public class ProtoBeamConverter {

  /** Returns a conversion method from Beam Row to Protobuf Message. */
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
        toProtos.put(
            fieldDescriptor.getName(), new ToProtoFieldSetter<Object, Object>(fieldDescriptor) {});
      }
    }
    return row -> {
      Schema schema = row.getSchema();
      DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor);
      for (Map.Entry<String, ToProtoSetter<?>> entry : toProtos.entrySet()) {
        String fieldName = entry.getKey();
        @SuppressWarnings("unchecked")
        ToProtoSetter<Object> converter = (ToProtoSetter<Object>) entry.getValue();
        converter.setToProto(
            message, schema.getField(fieldName).getType(), row.getValue(fieldName));
      }
      return message.build();
    };
  }

  /** Returns a conversion method from Protobuf Message to Beam Row. */
  public static SerializableFunction<@NonNull Message, @NonNull Row> toRow(Schema schema) {
    List<FromProtoGetter<?>> toBeams = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      Schema.FieldType fieldType = field.getType();
      if (fieldType.isLogicalType(OneOfType.IDENTIFIER)) {
        toBeams.add(new FromProtoOneOfGetter(field));
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

  static @NonNull ProtoConverter<?, ?> createToProtoConverter(
      Descriptors.FieldDescriptor fieldDescriptor) {
    if (fieldDescriptor.isRepeated()) {
      if (fieldDescriptor.isMapField()) {
        return new ProtoMapConverter<>(fieldDescriptor);
      } else {
        return new ProtoListConverter<>(fieldDescriptor);
      }
    } else {
      return createToProtoSingularConverter(fieldDescriptor);
    }
  }

  static @NonNull ProtoConverter<?, ?> createToProtoSingularConverter(
      Descriptors.FieldDescriptor fieldDescriptor) {
    switch (fieldDescriptor.getJavaType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case STRING:
        return new ProtoPassThroughConverter<>(fieldDescriptor);
      case BYTE_STRING:
        return new ProtoByteStringConverter(fieldDescriptor);
      case ENUM:
        return new ProtoEnumConverter(fieldDescriptor);
      case MESSAGE:
        String fullName = fieldDescriptor.getMessageType().getFullName();
        switch (fullName) {
          case "google.protobuf.Int32Value":
          case "google.protobuf.UInt32Value":
          case "google.protobuf.Int64Value":
          case "google.protobuf.UInt64Value":
          case "google.protobuf.FloatValue":
          case "google.protobuf.DoubleValue":
          case "google.protobuf.StringValue":
          case "google.protobuf.BoolValue":
            return new ProtoPassThroughConverter<>(fieldDescriptor);
          case "google.protobuf.BytesValue":
            return new ProtoByteStringConverter(fieldDescriptor);
          case "google.protobuf.Timestamp":
            return new ProtoTimestampConverter(fieldDescriptor);
          case "google.protobuf.Duration":
            return new ProtoDurationConverter(fieldDescriptor);
          case "google.protobuf.Any":
            throw new UnsupportedOperationException("google.protobuf.Any is not supported");
          default:
            return new ProtoMessageConverter(fieldDescriptor);
        }
      default:
        throw new UnsupportedOperationException(
            "Unsupported proto type: " + fieldDescriptor.getJavaType());
    }
  }

  interface FromProtoGetter<B> {
    @Nullable
    B getFromProto(Message message);
  }

  interface ToProtoSetter<B> {
    void setToProto(Message.Builder message, Schema.FieldType fieldType, B beamFieldValue);
  }

  abstract static class BeamConverter<P, B> {
    protected final Schema.FieldType fieldType;

    BeamConverter(Schema.FieldType fieldType) {
      this.fieldType = fieldType;
    }

    abstract @Nullable B convert(@Nullable P protoValue);
  }

  abstract static class BeamNoWrapConverter<P, B> extends BeamConverter<P, B> {
    BeamNoWrapConverter(Schema.FieldType fieldType) {
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

    abstract @NonNull B convertNonNull(@NonNull P protoValue);

    protected abstract @NonNull B defaultValue();
  }

  abstract static class BeamWrapConverter<P, U, B> extends BeamConverter<P, B> {

    BeamWrapConverter(Schema.FieldType fieldType) {
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

    abstract @NonNull B convertNonNullWrapped(@NonNull U protoValue);

    protected abstract @Nullable B defaultValue();
  }

  abstract static class ProtoConverter<B, P> {
    private final Descriptors.FieldDescriptor fieldDescriptor;

    ProtoConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
    }

    public abstract @Nullable P convert(@Nullable B beamValue);

    public Descriptors.FieldDescriptor getFieldDescriptor() {
      return fieldDescriptor;
    }
  }

  abstract static class ProtoNoWrapConverter<B> extends ProtoConverter<B, Object> {
    ProtoNoWrapConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public @Nullable Object convert(@Nullable B beamValue) {
      if (beamValue != null) {
        return convertNonNull(beamValue);
      } else {
        return null;
      }
    }

    protected abstract @NonNull Object convertNonNull(@NonNull B beamValue);
  }

  abstract static class ProtoWrapConverter<B, P> extends ProtoConverter<B, Object> {
    ProtoWrapConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public @Nullable Object convert(@Nullable B beamValue) {
      Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor();
      if (beamValue == null) {
        return null;
      }

      P protoValue = convertNonNullUnwrapped(beamValue);
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

    protected abstract @NonNull P convertNonNullUnwrapped(@NonNull B beamValue);
  }

  static class BeamBytesConverter extends BeamWrapConverter<Object, ByteString, byte[]> {
    public BeamBytesConverter(Schema.FieldType fieldType) {
      super(fieldType);
    }

    @Override
    protected byte @NonNull [] convertNonNullWrapped(@NonNull ByteString protoValue) {
      return protoValue.toByteArray();
    }

    @Override
    protected byte @NonNull [] defaultValue() {
      return new byte[0];
    }
  }

  static class BeamEnumerationConverter
      extends BeamNoWrapConverter<Descriptors.EnumValueDescriptor, EnumerationType.Value> {
    private final EnumerationType enumerationType;

    BeamEnumerationConverter(Schema.FieldType fieldType) {
      super(fieldType);
      enumerationType = fieldType.getLogicalType(EnumerationType.class);
    }

    @Override
    protected EnumerationType.@NonNull Value convertNonNull(
        Descriptors.@NonNull EnumValueDescriptor protoValue) {
      int number = protoValue.getNumber();
      return enumerationType.toInputType(number);
    }

    @Override
    protected EnumerationType.@NonNull Value defaultValue() {
      return enumerationType.toInputType(0);
    }
  }

  static class BeamListConverter<P, B>
      extends BeamNoWrapConverter<List<@NonNull P>, List<@NonNull B>> {
    private final BeamConverter<P, B> elementConverter;

    @SuppressWarnings("unchecked")
    public BeamListConverter(Schema.FieldType fieldType) {
      super(fieldType);
      elementConverter =
          (BeamConverter<P, B>)
              createBeamConverter(Preconditions.checkNotNull(fieldType.getCollectionElementType()));
    }

    @Override
    protected @NonNull List<@NonNull B> convertNonNull(@NonNull List<@NonNull P> protoList) {
      List<@NonNull B> beamList = new ArrayList<>();
      for (@NonNull P element : protoList) {
        beamList.add(Preconditions.checkNotNull(elementConverter.convert(element)));
      }
      return beamList;
    }

    @Override
    protected @NonNull List<@NonNull B> defaultValue() {
      return Collections.emptyList();
    }
  }

  static class BeamMapConverter<ProtoKeyT, ProtoValueT, BeamKeyT, BeamValueT>
      extends BeamNoWrapConverter<
          List<@NonNull Message>, Map<@NonNull BeamKeyT, @NonNull BeamValueT>> {
    private final BeamConverter<ProtoKeyT, BeamKeyT> keyConverter;
    private final BeamConverter<ProtoValueT, BeamValueT> valueConverter;

    @SuppressWarnings("unchecked")
    public BeamMapConverter(Schema.FieldType fieldType) {
      super(fieldType);
      keyConverter =
          (BeamConverter<ProtoKeyT, BeamKeyT>)
              createBeamConverter(Preconditions.checkNotNull(fieldType.getMapKeyType()));
      valueConverter =
          (BeamConverter<ProtoValueT, BeamValueT>)
              createBeamConverter(Preconditions.checkNotNull(fieldType.getMapValueType()));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected @NonNull Map<@NonNull BeamKeyT, @NonNull BeamValueT> convertNonNull(
        @NonNull List<@NonNull Message> protoList) {
      if (protoList.isEmpty()) {
        return Collections.emptyMap();
      }
      Descriptors.Descriptor descriptor = protoList.get(0).getDescriptorForType();
      Descriptors.FieldDescriptor keyFieldDescriptor = descriptor.findFieldByNumber(1);
      Descriptors.FieldDescriptor valueFieldDescriptor = descriptor.findFieldByNumber(2);
      Map<@NonNull BeamKeyT, @NonNull BeamValueT> beamMap = new HashMap<>();
      protoList.forEach(
          protoElement -> {
            ProtoKeyT protoKey =
                Preconditions.checkNotNull((ProtoKeyT) protoElement.getField(keyFieldDescriptor));
            ProtoValueT protoValue =
                Preconditions.checkNotNull(
                    (ProtoValueT) protoElement.getField(valueFieldDescriptor));
            BeamKeyT beamKey = Preconditions.checkNotNull(keyConverter.convert(protoKey));
            BeamValueT beamValue = Preconditions.checkNotNull(valueConverter.convert(protoValue));
            beamMap.put(beamKey, beamValue);
          });
      return beamMap;
    }

    @Override
    protected @NonNull Map<@NonNull BeamKeyT, @NonNull BeamValueT> defaultValue() {
      return Collections.emptyMap();
    }
  }

  static class BeamNanosDurationConverter extends BeamNoWrapConverter<Message, Duration> {
    BeamNanosDurationConverter(Schema.FieldType fieldType) {
      super(fieldType);
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

    @Override
    protected @NonNull Duration defaultValue() {
      return Duration.ZERO;
    }
  }

  static class BeamNanosInstantConverter extends BeamNoWrapConverter<Message, Instant> {
    BeamNanosInstantConverter(Schema.FieldType fieldType) {
      super(fieldType);
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

    @Override
    protected @NonNull Instant defaultValue() {
      return Instant.EPOCH;
    }
  }

  static class BeamPassThroughConverter<T> extends BeamWrapConverter<Object, T, T> {
    private final @NonNull T defaultValue;

    public BeamPassThroughConverter(Schema.FieldType fieldType, @NonNull T defaultValue) {
      super(fieldType);
      this.defaultValue = defaultValue;
    }

    @Override
    protected @NonNull T convertNonNullWrapped(@NonNull T protoValue) {
      Preconditions.checkArgument(protoValue.getClass().isInstance(defaultValue));
      return protoValue;
    }

    @Override
    protected @NonNull T defaultValue() {
      return defaultValue;
    }
  }

  static class BeamRowConverter extends BeamNoWrapConverter<Message, Row> {
    private final Schema rowSchema;
    private final SerializableFunction<@NonNull Message, @NonNull Row> converter;

    public BeamRowConverter(Schema.FieldType fieldType) {
      super(fieldType);
      rowSchema = Preconditions.checkNotNull(fieldType.getRowSchema());
      converter = toRow(rowSchema);
    }

    @Override
    protected @NonNull Row convertNonNull(@NonNull Message protoValue) {
      return converter.apply(protoValue);
    }

    @Override
    protected @NonNull Row defaultValue() {
      return Row.withSchema(rowSchema).build();
    }
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
      try {
        Descriptors.Descriptor descriptor = message.getDescriptorForType();
        Descriptors.FieldDescriptor fieldDescriptor =
            Preconditions.checkNotNull(descriptor.findFieldByName(field.getName()));

        @Nullable Object protoValue;
        if (field.getType().getNullable()
            && ProtoSchemaTranslator.isNullable(fieldDescriptor)
            && !message.hasField(fieldDescriptor)) {
          protoValue = null;
        } else {
          protoValue = message.getField(fieldDescriptor);
        }
        return converter.convert((P) protoValue);
      } catch (RuntimeException e) {
        throw new RuntimeException(
            String.format("Failed to get field from proto. field: %s", field.getName()), e);
      }
    }
  }

  static class FromProtoOneOfGetter implements FromProtoGetter<OneOfType.Value> {
    private final Schema.Field field;
    private final OneOfType oneOfType;
    private final Map<String, BeamConverter<Object, ?>> converter;

    FromProtoOneOfGetter(Schema.Field field) {
      this.field = field;
      this.oneOfType = Preconditions.checkNotNull(field.getType().getLogicalType(OneOfType.class));
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
        String subFieldName = entry.getKey();
        try {
          BeamConverter<Object, ?> value = entry.getValue();
          Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(subFieldName);
          if (message.hasField(fieldDescriptor)) {
            Object protoValue = Preconditions.checkNotNull(message.getField(fieldDescriptor));
            return oneOfType.createValue(subFieldName, value.convert(protoValue));
          }
        } catch (RuntimeException e) {
          throw new RuntimeException(
              String.format(
                  "Failed to get oneof from proto. oneof: %s, subfield: %s",
                  field.getName(), subFieldName),
              e);
        }
      }
      return null;
    }
  }

  static class ProtoByteStringConverter extends ProtoWrapConverter<byte[], ByteString> {
    ProtoByteStringConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    protected @NonNull ByteString convertNonNullUnwrapped(byte @NonNull [] beamValue) {
      return ByteString.copyFrom(beamValue);
    }
  }

  static class ProtoDurationConverter extends ProtoNoWrapConverter<Duration> {
    ProtoDurationConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    protected @NonNull Object convertNonNull(@NonNull Duration beamValue) {
      return com.google.protobuf.Duration.newBuilder()
          .setSeconds(beamValue.getSeconds())
          .setNanos(beamValue.getNano())
          .build();
    }
  }

  static class ProtoEnumConverter
      extends ProtoWrapConverter<EnumerationType.Value, Descriptors.EnumValueDescriptor> {
    ProtoEnumConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    protected Descriptors.@NonNull EnumValueDescriptor convertNonNullUnwrapped(
        EnumerationType.@NonNull Value beamValue) {
      return getFieldDescriptor().getEnumType().findValueByNumber(beamValue.getValue());
    }
  }

  static class ProtoListConverter<B, P> extends ProtoNoWrapConverter<Iterable<@NonNull B>> {
    private final @NonNull ProtoConverter<B, P> converter;

    @SuppressWarnings("unchecked")
    ProtoListConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
      converter = (ProtoConverter<B, P>) createToProtoSingularConverter(fieldDescriptor);
    }

    @Override
    protected @NonNull List<@NonNull P> convertNonNull(@NonNull Iterable<@NonNull B> beamValue) {
      List<@NonNull P> protoList = new ArrayList<>();
      for (@NonNull B beamElement : beamValue) {
        P protoElement = Preconditions.checkNotNull(converter.convert(beamElement));
        protoList.add(protoElement);
      }
      return protoList;
    }
  }

  static class ProtoMapConverter<BeamKeyT, BeamValueT, ProtoKeyT, ProtoValueT>
      extends ProtoNoWrapConverter<Map<BeamKeyT, BeamValueT>> {

    private final Descriptors.Descriptor mapDescriptor;
    private final Descriptors.FieldDescriptor keyDescriptor;
    private final Descriptors.FieldDescriptor valueDescriptor;
    private final ProtoConverter<BeamKeyT, ProtoKeyT> keyToProto;
    private final ProtoConverter<BeamValueT, ProtoValueT> valueToProto;

    @SuppressWarnings("unchecked")
    ProtoMapConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
      mapDescriptor = fieldDescriptor.getMessageType();
      keyDescriptor = mapDescriptor.findFieldByNumber(1);
      valueDescriptor = mapDescriptor.findFieldByNumber(2);
      keyToProto =
          (ProtoConverter<BeamKeyT, ProtoKeyT>) createToProtoSingularConverter(keyDescriptor);
      valueToProto =
          (ProtoConverter<BeamValueT, ProtoValueT>) createToProtoSingularConverter(valueDescriptor);
    }

    @Override
    protected @NonNull Object convertNonNull(@NonNull Map<BeamKeyT, BeamValueT> beamValue) {
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

  static class ProtoMessageConverter extends ProtoNoWrapConverter<Row> {
    private final SerializableFunction<Row, Message> converter;

    ProtoMessageConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
      this.converter = toProto(fieldDescriptor.getMessageType());
    }

    @Override
    protected @NonNull Object convertNonNull(@NonNull Row beamValue) {
      return converter.apply(beamValue);
    }
  }

  static class ProtoPassThroughConverter<T> extends ProtoWrapConverter<T, T> {
    ProtoPassThroughConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    protected @NonNull T convertNonNullUnwrapped(@NonNull T beamValue) {
      return beamValue;
    }
  }

  static class ProtoTimestampConverter extends ProtoNoWrapConverter<Instant> {
    ProtoTimestampConverter(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    protected @NonNull Object convertNonNull(@NonNull Instant beamValue) {
      return com.google.protobuf.Timestamp.newBuilder()
          .setSeconds(beamValue.getEpochSecond())
          .setNanos(beamValue.getNano())
          .build();
    }
  }

  static class ToProtoFieldSetter<B, P> implements ToProtoSetter<B> {
    private final @NonNull ProtoConverter<B, P> converter;

    @SuppressWarnings("unchecked")
    ToProtoFieldSetter(Descriptors.FieldDescriptor fieldDescriptor) {
      this.converter = (ProtoConverter<B, P>) createToProtoConverter(fieldDescriptor);
    }

    @Override
    public void setToProto(Message.Builder message, Schema.FieldType fieldType, B beamFieldValue) {
      try {
        @Nullable P protoValue = converter.convert(beamFieldValue);
        if (protoValue != null) {
          message.setField(converter.getFieldDescriptor(), protoValue);
        }
      } catch (RuntimeException e) {
        throw new RuntimeException(
            String.format(
                "Failed to set field to proto. field:%s", converter.getFieldDescriptor().getName()),
            e);
      }
    }
  }

  static class ToProtoOneOfSetter implements ToProtoSetter<OneOfType.Value> {
    private final Descriptors.OneofDescriptor oneofDescriptor;
    private final Map<String, ToProtoFieldSetter<Object, Object>> protoSetters;

    ToProtoOneOfSetter(Descriptors.OneofDescriptor oneofDescriptor) {
      this.oneofDescriptor = oneofDescriptor;
      this.protoSetters = createConverters(oneofDescriptor.getFields());
    }

    private static Map<String, ToProtoFieldSetter<Object, Object>> createConverters(
        List<Descriptors.FieldDescriptor> fieldDescriptors) {
      Map<String, ToProtoFieldSetter<Object, Object>> converters = new LinkedHashMap<>();
      for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
        Preconditions.checkState(!fieldDescriptor.isRepeated());
        converters.put(fieldDescriptor.getName(), new ToProtoFieldSetter<>(fieldDescriptor));
      }
      return converters;
    }

    @Override
    public void setToProto(
        Message.Builder message, Schema.FieldType fieldType, OneOfType.Value oneOfValue) {
      if (oneOfValue != null) {
        OneOfType oneOfType = fieldType.getLogicalType(OneOfType.class);
        int number = oneOfValue.getCaseType().getValue();
        try {
          String subFieldName =
              Preconditions.checkNotNull(oneOfType.getCaseEnumType().getEnumName(number));

          ToProtoFieldSetter<Object, Object> protoSetter =
              Preconditions.checkNotNull(
                  protoSetters.get(subFieldName), "No setter for field '%s'", subFieldName);
          protoSetter.setToProto(
              message,
              oneOfType.getOneOfSchema().getField(subFieldName).getType(),
              oneOfValue.getValue());
        } catch (RuntimeException e) {
          throw new RuntimeException(
              String.format(
                  "Failed to set oneof to proto. oneof: %s, number: %d",
                  oneofDescriptor.getName(), number),
              e);
        }
      }
    }
  }
}
