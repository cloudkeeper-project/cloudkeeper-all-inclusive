package com.svbio.workflow.bundles.marshalers;

import java.util.Objects;

/**
 * Wrapper for objects that are Jackson-serialized/deserialized as root-level objects.
 *
 * <p>Instances of this class will be serialized by Jackson just like the wrapped object, but with an additional type
 * property.
 */
final class TypeIdWrapper {
    private final Object object;

    private TypeIdWrapper(Object object) {
        this.object = Objects.requireNonNull(object);
    }

    /**
     * Creates a new wrapper object.
     *
     * @param object the object to wrap
     * @return the new wrapper object
     */
    static TypeIdWrapper wrap(Object object) {
        return new TypeIdWrapper(object);
    }

    /**
     * Returns the wrapped object.
     *
     * @return the wrapped object
     */
    Object unwrap() {
        return object;
    }
}
