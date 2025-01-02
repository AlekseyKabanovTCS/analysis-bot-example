package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ta4j.core.BarSeries;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        String token = args[0]; // t.z_15Vnscq****
        String instrumentUid = args[1]; // e6123145-9665-43e0-8413-cd61b8aa9b13
        int startYear = Integer.parseInt(args[2]); // 2018
        int endYear = Integer.parseInt(args[3]); // 2024
        Duration candlePeriod = Duration.ofMinutes(15);

        InvestApiClient apiClient = new InvestApiClient();
        CsvHistoryReader csvReader = new CsvHistoryReader();
        BarSeriesCreator seriesCreator = new BarSeriesCreator();

        List<CompletableFuture<Void>> futures = IntStream.rangeClosed(startYear, endYear)
                .mapToObj(year -> CompletableFuture.runAsync(() ->
                        apiClient.downloadHistoricalDataArchive(token, instrumentUid, year)))
                .toList();
        futures.forEach(CompletableFuture::join);
        List<BarData> bars = csvReader.readHistoricalData(instrumentUid, startYear, endYear);
        BarSeries series = seriesCreator.createBarSeries(bars, candlePeriod);
        log.info("Successfully created bar series with {} bars!", series.getBarCount());
    }
}
