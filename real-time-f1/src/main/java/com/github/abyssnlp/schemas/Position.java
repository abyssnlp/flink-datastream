package com.github.abyssnlp.schemas;

import java.time.Instant;

public record Position(
        Instant date,
        long session_key,
        long meeting_key,
        int driver_number,
        short position,
        long version
) {
    public String toClickHouseJson() {
        return ClickHouseJson.object(
                "date", ClickHouseJson.dateTime(date),
                "session_key", session_key,
                "meeting_key", meeting_key,
                "driver_number", driver_number,
                "position", position,
                "version", version
        );
    }
}
