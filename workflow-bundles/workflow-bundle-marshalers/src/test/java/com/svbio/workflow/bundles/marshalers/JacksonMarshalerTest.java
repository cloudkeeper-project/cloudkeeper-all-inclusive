package com.svbio.workflow.bundles.marshalers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.svbio.cloudkeeper.marshaling.DelegatingMarshalContext;
import com.svbio.cloudkeeper.marshaling.DelegatingUnmarshalContext;
import com.svbio.cloudkeeper.marshaling.MarshalingTreeBuilder;
import com.svbio.cloudkeeper.marshaling.MarshalingTreeNode;
import com.svbio.cloudkeeper.marshaling.MarshalingTreeNode.ByteSequenceNode;
import com.svbio.cloudkeeper.marshaling.MarshalingTreeNode.MarshaledReplacementObjectNode;
import com.svbio.cloudkeeper.marshaling.MarshalingTreeNode.ObjectNode;
import com.svbio.cloudkeeper.marshaling.MarshalingTreeUnmarshalSource;
import com.svbio.cloudkeeper.marshaling.TreeBuilderMarshalTarget;
import com.svbio.cloudkeeper.marshaling.UnmarshalSource;
import com.svbio.cloudkeeper.model.api.MarshalContext;
import com.svbio.cloudkeeper.model.api.Marshaler;
import com.svbio.cloudkeeper.model.api.MarshalingException;
import com.svbio.cloudkeeper.model.api.UnmarshalContext;
import com.svbio.cloudkeeper.model.immutable.element.Key;
import com.svbio.cloudkeeper.model.util.ByteSequences;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class JacksonMarshalerTest {
    private static final JacksonMarshaler JACKSON_SERIALIZATION = new JacksonMarshaler();

    /**
     * Dummy class for this test. Public fields are used to (a) save code and (b) make sure that no Jackson annotations
     * are needed, which would be missing in the separate class loader created in {@link #roundTrip()}.
     */
    public static class Person {
        public int age;
        public String name;

        public Person() { }

        public Person(int age, String name) {
            this.age = age;
            this.name = name;
        }

        @Override
        public boolean equals(@Nullable Object otherObject) {
            if (this == otherObject) {
                return true;
            } else if (otherObject == null || getClass() != otherObject.getClass()) {
                return false;
            }

            Person person = (Person) otherObject;
            return age == person.age
                && name.equals(person.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(age, name);
        }
    }

    public static class InvalidPerson extends Person {
        public InvalidPerson(int age, String name) {
            super(age, name);
        }

        @JsonProperty("age")
        public int getAgeTimes2() {
            return 2 * age;
        }

        @JsonProperty("age")
        public int getAgeTimes3() {
            return 3 * age;
        }
    }

    private static boolean shouldMarshalSelfContainedObject(List<Key> path, Marshaler<?> marshaler, Object object) {
        return path.isEmpty();
    }

    /**
     * Verifies that an object can be properly marshaled and unmarshaled.
     *
     * <p>This method also tests that the class loader returned by {@link UnmarshalContext#getClassLoader()} is properly
     * used to load classes during unmarshaling.
     */
    @Test
    public void roundTrip() throws Exception {
        try (URLClassLoader classLoader = URLClassLoader.newInstance(
                new URL[] { Person.class.getProtectionDomain().getCodeSource().getLocation() }, null)) {
            Class<?> clazz = classLoader.loadClass(Person.class.getName());
            Assert.assertNotEquals(clazz, Person.class);
            Object expected = clazz.getConstructor(int.class, String.class).newInstance(24, "Alice");
            Person person = new Person(24, "Alice");
            Assert.assertNotEquals(expected, person);

            MarshaledReplacementObjectNode tree = (MarshaledReplacementObjectNode) MarshalingTreeBuilder.marshal(
                person,
                Collections.singletonList(JACKSON_SERIALIZATION),
                JacksonMarshalerTest::shouldMarshalSelfContainedObject
            );

            // Verify that the tree representation is of the correct form
            String marshaledString = new String(((ByteSequenceNode) tree.getChild()).getByteSequence().toByteArray());
            Assert.assertTrue(
                marshaledString.contains("\"@class\"")
                    && marshaledString.contains(person.getClass().getName())
            );

            // Note that unmarshaled is an instance of the dynamically loaded Person class, hence we cannot have a cast
            // in the source code:
            // (Person) unmarshaled
            Object unmarshaled = MarshalingTreeUnmarshalSource.unmarshal(tree, classLoader);
            Assert.assertNotEquals(unmarshaled, person);
            Assert.assertNotSame(unmarshaled, expected);
            Assert.assertEquals(expected, unmarshaled);
        }
    }

    @Test
    public void putFailure() throws IOException {
        MarshalingTreeBuilder treeBuilder
            = MarshalingTreeBuilder.create(JacksonMarshalerTest::shouldMarshalSelfContainedObject);
        TreeBuilderMarshalTarget<MarshalingTreeNode> marshalTarget = TreeBuilderMarshalTarget.create(treeBuilder);
        try (MarshalContext marshalContext
                = DelegatingMarshalContext.create(JACKSON_SERIALIZATION, Collections.emptyList(), marshalTarget)) {
            JACKSON_SERIALIZATION.put(new InvalidPerson(24, "Bob"), marshalContext);
            Assert.fail("Expected exception.");
        } catch (MarshalingException exception) {
            Assert.assertTrue(exception.getCause() instanceof JsonProcessingException);
        }
    }

    @Test
    public void getFailure() throws IOException {
        ObjectNode invalidTree = MarshaledReplacementObjectNode.of(
            JACKSON_SERIALIZATION,
            ByteSequenceNode.of(ByteSequences.arrayBacked("{ Invalid JSON! }".getBytes(StandardCharsets.UTF_8)))
        );
        UnmarshalSource unmarshalSource = MarshalingTreeUnmarshalSource.create(invalidTree);
        UnmarshalContext unmarshalContext
            = DelegatingUnmarshalContext.create(unmarshalSource, getClass().getClassLoader());
        try {
            JACKSON_SERIALIZATION.get(unmarshalContext);
            Assert.fail("Expected exception.");
        } catch (MarshalingException exception) {
            Assert.assertTrue(exception.getCause() instanceof JsonProcessingException);
        }
    }
}
