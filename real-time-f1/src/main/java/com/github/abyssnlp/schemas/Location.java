package com.github.abyssnlp.schemas;

import java.time.Instant;

public record Location(
        Instant date,
        int driver_number,
        long session_key,
        long meeting_key,
        long x,
        long y,
        long z,
        long version
) {
    public String toClickHouseJson() {
        return ClickHouseJson.object(
                "date", ClickHouseJson.dateTime(date),
                "driver_number", driver_number,
                "session_key", session_key,
                "meeting_key", meeting_key,
                "x", x,
                "y", y,
                "z", z,
                "version", version
        );
    }
}
