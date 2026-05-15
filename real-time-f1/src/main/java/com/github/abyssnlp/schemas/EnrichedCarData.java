package com.github.abyssnlp.schemas;

import java.time.Instant;

public record EnrichedCarData(
        Instant date,
        int driver_number,
        long session_key,
        long meeting_key,
        int speed,
        short n_gear,
        int rpm,
        int throttle,
        int brake,
        String full_name,
        String name_acronym,
        String team_name,
        String headshot_url,
        long version
) {
    public String toClickHouseJson() {
        return ClickHouseJson.object(
                "date", ClickHouseJson.dateTime(date),
                "driver_number", driver_number,
                "session_key", session_key,
                "meeting_key", meeting_key,
                "speed", speed,
                "n_gear", n_gear,
                "rpm", rpm,
                "throttle", throttle,
                "brake", brake,
                "version", version,
                "full_name", full_name,
                "name_acronym", name_acronym,
                "team_name", team_name,
                "headshot_url", headshot_url
        );
    }
}