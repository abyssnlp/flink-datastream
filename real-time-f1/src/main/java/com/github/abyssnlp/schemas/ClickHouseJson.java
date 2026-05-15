package com.github.abyssnlp.schemas;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

final class ClickHouseJson {
    private static final DateTimeFormatter DATETIME64_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private ClickHouseJson() {
    }

    static String dateTime(Instant instant) {
        if (instant == null) {
            return null;
        }

        return DATETIME64_FORMATTER.format(instant);
    }

    static String object(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("JSON object requires key/value pairs.");
        }

        Map<String, Object> fields = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            fields.put((String) keyValues[i], keyValues[i + 1]);
        }

        try {
            return JSON_MAPPER.writeValueAsString(fields);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize ClickHouse JSON payload.", e);
        }
    }
}
