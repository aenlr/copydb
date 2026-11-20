package copydb.util;

import java.time.Duration;

public class Stopwatch {

    private long start;
    private long end;

    public Stopwatch() {
        start();
    }

    public void start() {
        start = System.nanoTime();
        end = -1;
    }

    public boolean isStopped() {
        return end >= start;
    }

    public Duration toDuration() {
        return Duration.ofNanos(toNanos());
    }

    public long toNanos() {
        return end >= start
            ? end - start
            : System.nanoTime() - start;
    }

    public long toMillis() {
        return toNanos() / 1_000_000;
    }

    public double toSeconds() {
        return toMillis() / 1000.0;
    }

    public void stop() {
        end = System.nanoTime();
    }

    @Override
    public String toString() {
        Duration dur = toDuration();
        long h = dur.toHours();
        long m = dur.toMinutesPart();
        if (h > 0) {
            return String.format("%d:%02d:%02d.%03d", h, m, dur.toSecondsPart(), dur.toMillisPart());
        } else {
            return String.format("%02d:%02d.%03d", m, dur.toSecondsPart(), dur.toMillisPart());
        }
    }

}
