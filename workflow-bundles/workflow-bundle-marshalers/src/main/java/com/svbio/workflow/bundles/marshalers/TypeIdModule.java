package com.svbio.workflow.bundles.marshalers;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.ClassNameIdResolver;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.impl.TypeWrappedSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;

/**
 * Jackson Module that adds a type property to the JSON root object.
 *
 * <p>While Jackson has a feature called "default typing" (see
 * {@link com.fasterxml.jackson.databind.ObjectMapper#enableDefaultTyping(com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping, JsonTypeInfo.As)}),
 * it is quite invasive and adds type information even where not needed. This module only adds type information for the
 * root object and has no other side effects.
 */
final class TypeIdModule extends SimpleModule {
    private static final long serialVersionUID = -3442485851991914455L;

    /**
     * Constructor.
     *
     * @param classPropertyName name of the property that holds the class name, e.g., {@code "@class"}
     */
    TypeIdModule(String classPropertyName) {
        TypeFactory typeFactory = TypeFactory.defaultInstance();
        JavaType objectType = typeFactory.constructType(Object.class);
        ClassNameIdResolver idResolver = new ClassNameIdResolver(objectType, typeFactory);
        AsPropertyTypeSerializer typeSerializer = new AsPropertyTypeSerializer(idResolver, null, classPropertyName);

        // typeIdVisible = false, i.e., we do not want to merge the type-id property back into the JSON before the
        // real deserializer is called.
        AsPropertyTypeDeserializer typeDeserializer
            = new AsPropertyTypeDeserializer(objectType, idResolver, classPropertyName, false, null);

        addSerializer(TypeIdWrapper.class, new SerializerImpl(typeSerializer));
        addDeserializer(TypeIdWrapper.class, new DeserializerImpl(typeDeserializer));
    }

    /**
     * Serializer for {@link TypeIdWrapper} instances.
     *
     * <p>This serializer always delegates to a {@link TypeWrappedSerializer} created from a fixed
     * {@link TypeSerializer} (passed as constructor argument) and the {@link JsonSerializer} returned for
     * {@link TypeIdWrapper#object}.
     */
    private static final class SerializerImpl extends JsonSerializer<TypeIdWrapper> {
        private final TypeSerializer typeSerializer;

        private SerializerImpl(TypeSerializer typeSerializer) {
            this.typeSerializer = typeSerializer;
        }

        @Override
        public void serialize(TypeIdWrapper wrapper, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException {
            Object wrappedObject = wrapper.unwrap();
            JsonSerializer<Object> valueSerializer
                = serializerProvider.findValueSerializer(wrappedObject.getClass(), null);
            TypeWrappedSerializer wrappedSerializer = new TypeWrappedSerializer(typeSerializer, valueSerializer);
            wrappedSerializer.serialize(wrappedObject, jsonGenerator, serializerProvider);
        }
    }

    /**
     * Deserializer for {@link TypeIdWrapper} instances.
     *
     * <p>This deserializer always delegates to the {@link TypeDeserializer} passed as constructor argument.
     */
    private static final class DeserializerImpl extends JsonDeserializer<TypeIdWrapper> {
        private final TypeDeserializer typeDeserializer;

        private DeserializerImpl(AsPropertyTypeDeserializer typeDeserializer) {
            this.typeDeserializer = typeDeserializer;
        }

        @Override
        public TypeIdWrapper deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException {
            Object object = typeDeserializer.deserializeTypedFromObject(jsonParser, context);
            return TypeIdWrapper.wrap(object);
        }
    }
}
