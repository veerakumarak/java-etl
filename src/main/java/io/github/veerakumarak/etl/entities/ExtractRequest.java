package io.github.veerakumarak.etl.entities;


import java.util.Map;

public record ExtractRequest(
        String source,
        String group,
        String job,
        Map<String, String> data,
        Integer fetchSize
) {
}
