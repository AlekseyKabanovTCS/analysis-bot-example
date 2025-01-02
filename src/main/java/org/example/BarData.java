package org.example;

import java.time.ZonedDateTime;

public record BarData(
        String startTime,
        double low,
        double high,
        double open,
        double close,
        double volume
) {
}
