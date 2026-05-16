package com.github.abyssnlp.schemas;

import java.time.Instant;

public record EnrichedPosition(
        Instant date,
        long session_key,
        long meeting_key,
        int driver_number,
        short position,
        String full_name,
        String name_acronym,
        String team_name,
        String headshot_url,
        long version
) {
    public String toClickHouseJson() {
        return ClickHouseJson.object(
                "date", ClickHouseJson.dateTime(date),
                "session_key", session_key,
                "meeting_key", meeting_key,
                "driver_number", driver_number,
                "position", position,
                "full_name", full_name,
                "name_acronym", name_acronym,
                "team_name", team_name,
                "headshot_url", headshot_url,
                "version", version
        );
    }
}
