package io.github.veerakumarak.etl.compare;

import io.github.veerakumarak.etl.parquet.ClassHelper;
import io.github.veerakumarak.etl.parquet.GroupToClassConverter;
import io.github.veerakumarak.fp.failures.InternalFailure;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GroupHelper {

    private static boolean areGroupsEqual(Group g1, Group g2, Set<String> ignoredColumns) {
        if (g1 == g2) return true;
        if (g1 == null || g2 == null) return false;

        GroupType schema = g1.getType();
        if (!schema.equals(g2.getType())) return false;

        int fieldCount = schema.getFieldCount();

        for (int i = 0; i < fieldCount; i++) {
            // 1. Get the field name and check if it should be skipped
            String fieldName = schema.getFieldName(i);
            if (ignoredColumns != null && ignoredColumns.contains(fieldName)) {
                continue;
            }

            int valCount1 = g1.getFieldRepetitionCount(i);
            int valCount2 = g2.getFieldRepetitionCount(i);

            if (valCount1 != valCount2) return false;

            for (int j = 0; j < valCount1; j++) {
                Type type = schema.getType(i);

                if (type.isPrimitive()) {
                    // Compare primitives based on their actual value
                    if (!g1.getValueToString(i, j).equals(g2.getValueToString(i, j))) {
                        return false;
                    }
                } else {
                    // Recursively compare nested Groups
                    if (!areGroupsEqual(g1.getGroup(i, j), g2.getGroup(i, j), ignoredColumns)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static Comparator<Group> getComparator(Set<String> ignoredColumns) {
        return (g1, g2) -> areGroupsEqual(g1, g2, ignoredColumns) ? 0 : 1;
    }


    private static int calculateHash(Group group, Set<String> ignoredColumns) {
        if (group == null) return 0;

        GroupType schema = group.getType();
        int result = 1;

        for (int i = 0; i < schema.getFieldCount(); i++) {
            String fieldName = schema.getFieldName(i);

            // Skip ignored columns
            if (ignoredColumns != null && ignoredColumns.contains(fieldName)) {
                continue;
            }

            int fieldRepetitionCount = group.getFieldRepetitionCount(i);
            for (int j = 0; j < fieldRepetitionCount; j++) {
                Type type = schema.getType(i);
                Object value;

                if (type.isPrimitive()) {
                    // For primitives, we use the String representation or raw value
                    value = group.getValueToString(i, j);
                } else {
                    // Recursively hash nested groups
                    value = calculateHash(group.getGroup(i, j), ignoredColumns);
                }

                // Standard hash combining: result = 31 * result + (value == null ? 0 : value.hashCode())
                result = 31 * result + (value == null ? 0 : value.hashCode());
            }
        }
        return result;
    }

    public static Function<Group, Integer> getHashFunction(Set<String> ignoredColumns) {
        return (g) -> calculateHash(g, ignoredColumns);
    }

    public <T> Function<Group, Integer> createHashFunction(Class<T> tClass) {

        Field[] classFields = tClass.getDeclaredFields();

        Constructor<?> constructor = ClassHelper.getAllArgsConstructor(tClass)
                .orElseThrow(() -> new InternalFailure("AllArgsConstructor missing: " + tClass.getName()));

        return (Group group) -> {
            if (group == null) return 0;

            Map<String, Type> fieldsMap = group.getType().getFields().stream()
                    .collect(Collectors.toMap(Type::getName, field -> field));

            Object[] values = GroupToClassConverter.convert(group, classFields, fieldsMap, false);
            T object = null;
            try {
                object = (T) constructor.newInstance(values);
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            }

            // 1. Convert Group to the Generic Type T
            // Assuming ParquetReaderHelper or a similar mapper exists
//            T object = ParquetMapper.convertToEntity(group, tClass);

            // 2. Return the hashCode of the converted object
            return (object != null) ? object.hashCode() : 0;
        };
    }
}
