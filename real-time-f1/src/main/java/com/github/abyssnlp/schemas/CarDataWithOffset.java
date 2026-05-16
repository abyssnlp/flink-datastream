package com.github.abyssnlp.schemas;

public record CarDataWithOffset(
        CarData carData,
        long offset
)
{}
