package io.github.veerakumarak.etl.parquet.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME) // Make sure the annotation is available at runtime for reflection
@Target(ElementType.FIELD)     // Apply this annotation to constructor parameters
public @interface DataField {
    String value();
}
