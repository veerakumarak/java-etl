package io.github.veerakumarak.etl.compare;

import java.util.Comparator;
import java.util.Objects;
import java.util.function.Function;

public class TypedHelper {

    public static <T> Comparator<T> getComparator() {
        return (t1, t2) -> Objects.equals(t1, t2) ? 0 : 1;
    }

    public static <T> Function<T, Integer> getHashFunction() {
        return Objects::hashCode;
    }

}
