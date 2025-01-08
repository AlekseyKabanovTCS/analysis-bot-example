package org.example;

public class BarData {
        private final String startTime;
        private final double low;
        private final double high;
        private final double open;
        private final double close;
        private final long volume;

        public BarData(String startTime, double low, double high, double open, double close, long volume) {
            this.startTime = startTime;
            this.low = low;
            this.high = high;
            this.open = open;
            this.close = close;
            this.volume = volume;
        }

    public String getStartTime() {
        return startTime;
    }

    public double getLow() {
        return low;
    }

    public double getHigh() {
        return high;
    }

    public double getOpen() {
        return open;
    }

    public double getClose() {
        return close;
    }

    public long getVolume() {
        return volume;
    }
}
