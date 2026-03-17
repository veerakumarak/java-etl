package io.github.veerakumarak.etl.parquet.data;

import java.lang.reflect.Field;

public class DataAnnotationHelper {

    public static String getName(Field filed) {
        String name = filed.getName();
        if (filed.isAnnotationPresent(DataField.class)) {
            DataField annotation = filed.getAnnotation(DataField.class);
            name = annotation.value();
        }
        return name;
    }

}
