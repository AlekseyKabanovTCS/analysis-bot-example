package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ta4j.core.BarSeries;
import ru.tinkoff.piapi.contract.v1.CandleInterval;
import ru.tinkoff.piapi.contract.v1.HistoricCandle;
import ru.tinkoff.piapi.core.InvestApi;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        String token = args[0]; // t.z_15Vnscq****
        String instrumentUid = args[1]; // e6123145-9665-43e0-8413-cd61b8aa9b13
        int startYear = Integer.parseInt(args[2]); // 2018
        int endYear = Integer.parseInt(args[3]); // 2024
        CandleInterval candleInterval = CandleInterval.CANDLE_INTERVAL_30_MIN;

        InvestApiClient apiClient = new InvestApiClient();
        CsvHistoryReader csvReader = new CsvHistoryReader();
        BarSeriesCreator seriesCreator = new BarSeriesCreator();

        var startTime = ZonedDateTime.parse("2025-01-03T00:00:00Z").toInstant();
        var endTime = ZonedDateTime.parse("2025-01-04T00:00:00Z").toInstant();
        var candles = getCandles(token, instrumentUid, startTime, endTime, candleInterval);
        var testSeries = seriesCreator.createBarSeriesFromHistoricCandles(candles, candleInterval);

        List<CompletableFuture<Void>> futures = IntStream.rangeClosed(startYear, endYear)
                .mapToObj(year -> CompletableFuture.runAsync(() ->
                        apiClient.downloadHistoricalDataArchive(token, instrumentUid, year)))
                .collect(Collectors.toList());
        futures.forEach(CompletableFuture::join);
        List<BarData> bars = csvReader.readHistoricalData(instrumentUid, startYear, endYear);
        BarSeries series = seriesCreator.createBarSeriesFromBars(bars, candleInterval);
        log.info("Successfully created bar series with {} bars!", series.getBarCount());
    }

    public static List<HistoricCandle> getCandles(
            String token,
            String instrumentUid,
            Instant from,
            Instant to,
            CandleInterval candleInterval
    ) {
        var api = InvestApi.create(token);
        var service = api.getMarketDataService();
        var candles1min = service.getCandlesSync(instrumentUid, from, to, candleInterval);
        api.destroy(10);
        return candles1min;
    }
}
