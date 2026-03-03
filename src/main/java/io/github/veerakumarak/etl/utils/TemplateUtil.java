package io.github.veerakumarak.etl.utils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TemplateUtil {

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{\\{(\\w+)\\}\\}");

    public static String render(String template, Map<String, String> variables) {
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(template);
        StringBuilder stringBuilder = new StringBuilder();

        while (matcher.find()) {
            String key = matcher.group(1).trim();
            String value = variables.getOrDefault(key, "");
            matcher.appendReplacement(stringBuilder, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(stringBuilder);
        return stringBuilder.toString();
    }
}
