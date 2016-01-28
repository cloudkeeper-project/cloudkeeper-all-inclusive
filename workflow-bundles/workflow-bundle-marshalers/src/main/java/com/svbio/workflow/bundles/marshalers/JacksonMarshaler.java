package com.svbio.workflow.bundles.marshalers;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.svbio.cloudkeeper.dsl.SerializationPlugin;
import com.svbio.cloudkeeper.model.api.MarshalContext;
import com.svbio.cloudkeeper.model.api.Marshaler;
import com.svbio.cloudkeeper.model.api.MarshalingException;
import com.svbio.cloudkeeper.model.api.UnmarshalContext;
import com.svbio.cloudkeeper.model.immutable.element.NoKey;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * CloudKeeper marshaler plugin for marshaling arbitrary objects to JSON using Jackson.
 *
 * <p>This marshaler implementation does not write out any explicit type information using {@link Marshaler} methods.
 * Instead, objects are wrapped in a special instance for which the Jackson {@link ObjectMapper} is configured to write
 * out type information as an additional JSON field.
 */
@SerializationPlugin("Marshaling arbitrary objects to JSON using Jackson")
public final class JacksonMarshaler implements Marshaler<Object> {
    private final ObjectWriter objectWriter;
    private final ObjectReader objectReader;

    /**
     * No-argument constructor.
     */
    public JacksonMarshaler() {
        TypeIdModule typeIdModule = new TypeIdModule("@class");
        ObjectMapper objectMapper = new ObjectMapper().registerModule(typeIdModule);
        // See PF-769 and https://github.com/FasterXML/jackson-core/issues/207 for an explanation of the following line
        objectMapper.getFactory().disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES);
        objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
        objectReader = objectMapper.reader().forType(TypeIdWrapper.class);
    }

    @Override
    public boolean isImmutable(Object object) {
        return false;
    }

    @Override
    public void put(Object object, MarshalContext context) throws IOException {
        try (OutputStream outputStream = context.newOutputStream(NoKey.instance())) {
            objectWriter.writeValue(outputStream, TypeIdWrapper.wrap(object));
        } catch (JsonProcessingException exception) {
            throw new MarshalingException("Failed to generate JSON.", exception);
        }
    }

    @Override
    public Object get(UnmarshalContext context) throws IOException {
        Thread currentThread = Thread.currentThread();
        ClassLoader oldClassLoader = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(context.getClassLoader());
        try (InputStream inputStream = context.getByteSequence(NoKey.instance()).newInputStream()) {
            TypeIdWrapper wrapper = objectReader.readValue(inputStream);
            return wrapper.unwrap();
        } catch (JsonProcessingException exception) {
            throw new MarshalingException("Failed to parse JSON.", exception);
        } finally {
            currentThread.setContextClassLoader(oldClassLoader);
        }
    }
}
