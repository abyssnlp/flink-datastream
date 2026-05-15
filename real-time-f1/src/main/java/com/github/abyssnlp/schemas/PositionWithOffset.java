package com.github.abyssnlp.schemas;

public record PositionWithOffset(
        Position position,
        long offset
) {
}
