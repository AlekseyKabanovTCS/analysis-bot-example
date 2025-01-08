package org.example;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;

public class CsvHistoryReader {

    private static final Logger log = LoggerFactory.getLogger(CsvHistoryReader.class);
    private static final String FILENAME_PATTERN = "%s_%d.zip";

    public List<BarData> readHistoricalData(String instrumentUid, int startYear, int stopYear) {
        long startTime = System.currentTimeMillis();
        List<BarData> bars = new LinkedList<>();
        IntStream.rangeClosed(startYear, stopYear)
                .mapToObj(year -> String.format(FILENAME_PATTERN, instrumentUid, year))
                .forEach(fileName -> {
                    try (ZipFile zip = ZipFile.builder().setFile(fileName).get()) {
                        Collections.list(zip.getEntries()).stream()
                                .sorted(Comparator.comparing(ZipEntry::getName))
                                .forEach(entry -> addEntryToSeries(zip, entry, bars));
                    } catch (IOException e) {
                        log.error("Error occurred while reading file: {}", fileName);
                    }
                });
        long duration = System.currentTimeMillis() - startTime;
        log.info("Data was successfully read within {} ms. Bars count: {}", duration, bars.size());
        return bars;
    }

    public void addEntryToSeries(ZipFile zip, ZipArchiveEntry entry, List<BarData> bars) {
        if (!entry.isDirectory()) {
            String fileName = entry.getName();
            try (InputStream inputStream = zip.getInputStream(entry);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    bars.add(parseBarDataFromLine(line));
                }
            } catch (IOException e) {
                log.error("Error occurred while reading zip data: {}", e.getMessage());
            }
            log.trace("Successfully fetched zip entry! Filename: {}", fileName);
        }
    }

    public BarData parseBarDataFromLine(String line) {
        String[] data = line.split(";");
        return new BarData(
                data[1], // start time
                Double.parseDouble(data[5]), // low
                Double.parseDouble(data[4]), // high
                Double.parseDouble(data[2]), // open
                Double.parseDouble(data[3]), // close
                Long.parseLong(data[6]) //volume
        );
    }
}
