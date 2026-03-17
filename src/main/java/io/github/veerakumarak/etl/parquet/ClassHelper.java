package io.github.veerakumarak.etl.parquet;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ClassHelper {

    public static Optional<Constructor<?>> getAllArgsConstructor(Class<?> recordClass) {
        Field[] classFields = recordClass.getDeclaredFields();

        long numExpectedArgs = Arrays.stream(classFields)
                .filter(f -> !Modifier.isStatic(f.getModifiers()) && !f.isSynthetic())
                .count();

        List<Constructor<?>> candidateConstructors = Arrays.stream(recordClass.getConstructors())
                .filter(c -> c.getParameterCount() == numExpectedArgs)
                .toList();

        // Use get(0) instead of getFirst() for Java 17 compatibility
        return candidateConstructors.size() == 1 ? Optional.of(candidateConstructors.get(0)) : Optional.empty();
    }

}
