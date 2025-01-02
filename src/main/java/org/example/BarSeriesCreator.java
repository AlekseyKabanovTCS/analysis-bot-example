package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBar;
import org.ta4j.core.BaseBarSeriesBuilder;
import org.ta4j.core.num.DoubleNum;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.ListIterator;

public class BarSeriesCreator {

    private static final Logger log = LoggerFactory.getLogger(BarSeriesCreator.class);

    public BarSeries createBarSeries(List<BarData> bars, Duration candlePeriod) {
        long startTime = System.currentTimeMillis();
        BarSeries series = new BaseBarSeriesBuilder().withNumTypeOf(DoubleNum.class).build();
        long periodMinutes = candlePeriod.toMinutes();
        ListIterator<BarData> iterator = bars.listIterator();
        while (iterator.hasNext()) {
            BarData firstBar = iterator.next();
            ZonedDateTime endTime = ZonedDateTime.parse(firstBar.startTime()).plus(candlePeriod);
            BarData aggregatedBar = aggregateBars(iterator, firstBar, endTime, periodMinutes);
            addBarToSeries(series, aggregatedBar, candlePeriod);
        }
        long duration = System.currentTimeMillis() - startTime;
        log.info("BarSeries was successfully created within {} ms", duration);
        return series;
    }

    private BarData aggregateBars(
            ListIterator<BarData> iterator,
            BarData firstBar,
            ZonedDateTime endTime,
            long periodMinutes
    ) {
        String endTimeStr = endTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME);
        double low = firstBar.low();
        double high = firstBar.high();
        double volume = firstBar.volume();
        double close = firstBar.close();
        if (periodMinutes > 1) {
            while (iterator.hasNext()) {
                BarData currentBar = iterator.next();
                if (currentBar.startTime().compareTo(endTimeStr) >= 0) {
                    iterator.previous();
                    break;
                }
                low = Math.min(low, currentBar.low());
                high = Math.max(high, currentBar.high());
                volume += currentBar.volume();
                close = currentBar.close();
            }
        }
        return new BarData(firstBar.startTime(), low, high, firstBar.open(), close, volume);
    }

    private void addBarToSeries(BarSeries series, BarData aggregatedBar, Duration candlePeriod) {
        ZonedDateTime endTime = ZonedDateTime.parse(aggregatedBar.startTime()).plus(candlePeriod);
        series.addBar(BaseBar.builder()
                .endTime(endTime)
                .openPrice(DoubleNum.valueOf(aggregatedBar.open()))
                .closePrice(DoubleNum.valueOf(aggregatedBar.close()))
                .highPrice(DoubleNum.valueOf(aggregatedBar.high()))
                .lowPrice(DoubleNum.valueOf(aggregatedBar.low()))
                .volume(DoubleNum.valueOf(aggregatedBar.volume()))
                .timePeriod(candlePeriod)
                .build());
    }
}
