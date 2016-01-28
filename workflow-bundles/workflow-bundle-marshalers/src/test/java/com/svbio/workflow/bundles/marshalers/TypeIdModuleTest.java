package com.svbio.workflow.bundles.marshalers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class TypeIdModuleTest {
    @Nullable private ObjectWriter writer;
    @Nullable private ObjectReader reader;

    @BeforeClass
    public void setup() {
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new TypeIdModule("@class"));
        writer = objectMapper.writerWithDefaultPrettyPrinter();
        reader = objectMapper.readerFor(TypeIdWrapper.class);
    }

    private static class Sequence {
        @Nullable public List<Integer> numbers;

        @Override
        public boolean equals(@Nullable Object otherObject) {
            return this == otherObject
                || (
                    otherObject != null
                    && getClass() == otherObject.getClass()
                    && Objects.equals(numbers, ((Sequence) otherObject).numbers)
                );
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(numbers);
        }
    }

    private static class AnotherClass {
        @Nullable public String name;
        @Nullable public Sequence sequence;

        @Override
        public boolean equals(@Nullable Object otherObject) {
            if (this == otherObject) {
                return true;
            } else if (otherObject == null || getClass() != otherObject.getClass()) {
                return false;
            }

            AnotherClass other = (AnotherClass) otherObject;
            return Objects.equals(name, other.name)
                && Objects.equals(sequence, other.sequence);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, sequence);
        }
    }

    private <T> T roundTrip(T object) throws IOException {
        assert writer != null && reader != null;
        String string = writer.writeValueAsString(TypeIdWrapper.wrap(object));
        TypeIdWrapper wrapper = reader.readValue(string);
        @SuppressWarnings("unchecked")
        T reconstructed = (T) wrapper.unwrap();
        return reconstructed;
    }

    @Test
    public void testSingleProperty() throws IOException {
        Sequence sequence = new Sequence();
        sequence.numbers = Arrays.asList(12, 23);

        Sequence deserializedSequence = roundTrip(sequence);
        Assert.assertNotSame(deserializedSequence, sequence);
        Assert.assertEquals(deserializedSequence, sequence);
    }

    @Test
    public void testNestedObject() throws IOException {
        Sequence sequence = new Sequence();
        sequence.numbers = Arrays.asList(12, 23);
        AnotherClass anotherObject = new AnotherClass();
        anotherObject.name = "Lifecode";
        anotherObject.sequence = sequence;

        AnotherClass deserializedObject = roundTrip(anotherObject);
        Assert.assertNotSame(deserializedObject, anotherObject);
        Assert.assertEquals(deserializedObject, anotherObject);
    }
}
