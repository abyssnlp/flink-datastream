package com.github.abyssnlp.schemas;

public record Driver (
        int meeting_key,
        int session_key,
        int driver_number,
        String full_name,
        String name_acronym,
        String team_name,
        String headshot_url
) {
    public String toClickHouseJson() {
        return ClickHouseJson.object(
                "meeting_key", meeting_key,
                "session_key", session_key,
                "driver_number", driver_number,
                "name_acronym", name_acronym,
                "full_name", full_name,
                "team_name", team_name,
                "headshot_url", headshot_url
        );
    }
}
