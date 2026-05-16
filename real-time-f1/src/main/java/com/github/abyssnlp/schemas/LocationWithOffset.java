package com.github.abyssnlp.schemas;

public record LocationWithOffset(
        Location location,
        long offset
) {
}
