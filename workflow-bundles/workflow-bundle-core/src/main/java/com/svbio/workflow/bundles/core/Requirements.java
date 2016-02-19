package com.svbio.workflow.bundles.core;

import xyz.cloudkeeper.dsl.AnnotationTypePlugin;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation type for specifying the resource requirements of a CloudKeeper module.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.TYPE })
@AnnotationTypePlugin("Annotation specifying the resource requirements of a CloudKeeper module.")
public @interface Requirements {
    /**
     * The number of CPU cores that should be reserved.
     */
    int cpuCores();

    /**
     * The amount of main memory (in GB) that should be reserved.
     */
    int memoryGB();
}
