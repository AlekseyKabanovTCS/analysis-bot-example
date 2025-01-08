package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBar;
import org.ta4j.core.BaseBarSeriesBuilder;
import org.ta4j.core.num.DoubleNum;
import ru.tinkoff.piapi.contract.v1.CandleInterval;
import ru.tinkoff.piapi.contract.v1.HistoricCandle;
import ru.tinkoff.piapi.core.utils.DateUtils;
import ru.tinkoff.piapi.core.utils.MapperUtils;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.ListIterator;

public class BarSeriesCreator {

    private static final Logger log = LoggerFactory.getLogger(BarSeriesCreator.class);

    public BarSeries createBarSeriesFromBars(List<BarData> bars, CandleInterval candleInterval) {
        long startTimeMs = System.currentTimeMillis();
        BarSeries series = new BaseBarSeriesBuilder().withNumTypeOf(DoubleNum.class).build();
        ListIterator<BarData> iterator = bars.listIterator();
        while (iterator.hasNext()) {
            BarData firstBar = iterator.next();
            ZonedDateTime startTime = ZonedDateTime.parse(firstBar.getStartTime());
            ZonedDateTime endTime = getEndTime(startTime, candleInterval);
            BarData aggregatedBar = aggregateBars(iterator, firstBar, endTime, candleInterval);
            addBarToSeries(series, aggregatedBar, endTime);
        }
        long duration = System.currentTimeMillis() - startTimeMs;
        log.info("BarSeries was successfully created within {} ms", duration);
        return series;
    }

    public BarSeries createBarSeriesFromHistoricCandles(List<HistoricCandle> candles, CandleInterval candleInterval) {
        BarSeries series = new BaseBarSeriesBuilder().withNumTypeOf(DoubleNum.class).build();
        candles.forEach(candle -> {
            var startTime = ZonedDateTime.ofInstant(
                    DateUtils.timestampToInstant(candle.getTime()),
                    ZoneId.of("Etc/GMT")
            );
            var endTime = getEndTime(startTime, candleInterval);
            series.addBar(BaseBar.builder()
                    .endTime(endTime)
                    .openPrice(DoubleNum.valueOf(MapperUtils.quotationToBigDecimal(candle.getOpen())))
                    .closePrice(DoubleNum.valueOf(MapperUtils.quotationToBigDecimal(candle.getClose())))
                    .lowPrice(DoubleNum.valueOf(MapperUtils.quotationToBigDecimal(candle.getLow())))
                    .highPrice(DoubleNum.valueOf(MapperUtils.quotationToBigDecimal(candle.getHigh())))
                    .volume(DoubleNum.valueOf(candle.getVolume()))
                    .timePeriod(Duration.between(startTime, endTime))
                    .build());
        });
        return series;
    }

    private BarData aggregateBars(
            ListIterator<BarData> iterator,
            BarData firstBar,
            ZonedDateTime endTime,
            CandleInterval candleInterval
    ) {
        String endTimeStr = endTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME);
        double low = firstBar.getLow();
        double high = firstBar.getHigh();
        double close = firstBar.getClose();
        long volume = firstBar.getVolume();
        if (candleInterval.getNumber() > 1) {
            while (iterator.hasNext()) {
                BarData currentBar = iterator.next();
                if (currentBar.getStartTime().compareTo(endTimeStr) >= 0) {
                    iterator.previous();
                    break;
                }
                low = Math.min(low, currentBar.getLow());
                high = Math.max(high, currentBar.getHigh());
                volume += currentBar.getVolume();
                close = currentBar.getClose();
            }
        }
        return new BarData(firstBar.getStartTime(), low, high, firstBar.getOpen(), close, volume);
    }

    private void addBarToSeries(BarSeries series, BarData aggregatedBar,ZonedDateTime endTime) {
        series.addBar(BaseBar.builder()
                .endTime(endTime)
                .openPrice(DoubleNum.valueOf(aggregatedBar.getOpen()))
                .closePrice(DoubleNum.valueOf(aggregatedBar.getClose()))
                .highPrice(DoubleNum.valueOf(aggregatedBar.getHigh()))
                .lowPrice(DoubleNum.valueOf(aggregatedBar.getLow()))
                .volume(DoubleNum.valueOf(aggregatedBar.getVolume()))
                .timePeriod(Duration.between(ZonedDateTime.parse(aggregatedBar.getStartTime()), endTime))
                .build());
    }

    public ZonedDateTime getEndTime(ZonedDateTime startTime, CandleInterval candleInterval) {
        // TODO: дописать округление для младших таймфреймов
        switch (candleInterval) {
            case CANDLE_INTERVAL_1_MIN:
                return startTime.plus(1, ChronoUnit.MINUTES);
            case CANDLE_INTERVAL_2_MIN:
                return startTime.plus(2, ChronoUnit.MINUTES);
            case CANDLE_INTERVAL_3_MIN:
                return startTime.plus(3, ChronoUnit.MINUTES);
            case CANDLE_INTERVAL_5_MIN:
                return startTime.plus(5, ChronoUnit.MINUTES);
            case CANDLE_INTERVAL_10_MIN:
                return startTime.plus(10, ChronoUnit.MINUTES);
            case CANDLE_INTERVAL_15_MIN:
                return startTime.plus(15, ChronoUnit.MINUTES);
            case CANDLE_INTERVAL_30_MIN:
                return startTime.plus(30, ChronoUnit.MINUTES);
            case CANDLE_INTERVAL_HOUR:
                return startTime.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS);
            case CANDLE_INTERVAL_2_HOUR:
                return startTime.truncatedTo(ChronoUnit.HOURS).plus(2, ChronoUnit.HOURS);
            case CANDLE_INTERVAL_4_HOUR:
                return startTime.truncatedTo(ChronoUnit.HOURS).plus(4, ChronoUnit.HOURS);
            case CANDLE_INTERVAL_DAY:
                return startTime.truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.DAYS);
            case CANDLE_INTERVAL_WEEK:
                return startTime.truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.WEEKS);
            case CANDLE_INTERVAL_MONTH:
                return startTime.truncatedTo(ChronoUnit.MONTHS).plus(1, ChronoUnit.MONTHS);
            default:
                return startTime;
        }
    }
}
