package org.example;

import com.google.common.net.HttpHeaders;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class InvestApiClient {

    private static final String HISTORY_DATA_URL = "https://invest-public-api.tinkoff.ru/history-data?instrumentId=%s&year=%d";
    private static final Logger log = LoggerFactory.getLogger(InvestApiClient.class);
    private static final String FILENAME_PATTERN = "%s_%d.zip";
    private static final OkHttpClient client = new OkHttpClient();

    public void downloadHistoricalDataArchive(String token, String instrumentUid, int year) {
        String fileName = FILENAME_PATTERN.formatted(instrumentUid, year);
        File file = new File(fileName);
        if (file.exists()) {
            log.info("File already exists, skipping download: {}", fileName);
            return;
        }
        Request request = new Request.Builder()
                .url(HISTORY_DATA_URL.formatted(instrumentUid, year))
                .addHeader(HttpHeaders.AUTHORIZATION, "Bearer %s".formatted(token))
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response);
            }
            saveResponseToFile(response, fileName);
        } catch (IOException e) {
            log.error("Error occurred while downloading data: {}", e.getMessage());
        }
    }

    public void saveResponseToFile(Response response, String filename) throws IOException {
        if (response.body() == null) {
            throw new IOException("Response body is null " + response);
        }
        File zipFile = new File(filename);
        try (ResponseBody responseBody = response.body();
             InputStream inputStream = responseBody.byteStream();
             FileOutputStream fileOutputStream = new FileOutputStream(zipFile)) {

            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }
        }
        log.info("ZIP file downloaded successfully!");
    }
}
