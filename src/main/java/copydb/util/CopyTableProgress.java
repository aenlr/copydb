package copydb.util;

import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;

public class CopyTableProgress {

    private final String name;
    private final long rowCount;
    private final String rowCountFormat;
    private final String rowCountFormat2;
    private final Stopwatch stopwatch;
    private long row;
    private long lastUpdateTime;
    private final AttributedStyle faintStyle = AttributedStyle.DEFAULT
        .foreground(AttributedStyle.WHITE)
        .faint();

    public CopyTableProgress(String name, long rowCount) {
        this.name = name;
        this.rowCount = rowCount;
        this.rowCountFormat = "[%" + String.valueOf(rowCount).length() + "d/%d]";
        this.rowCountFormat2 = "[%" + String.valueOf(rowCount).length() + "d/%d] [%.2f/s]";
        this.stopwatch = new Stopwatch();
    }

    public Stopwatch getStopwatch() {
        return stopwatch;
    }

    public void stop() {
        stopwatch.stop();
    }

    public void increment() {
        row++;
    }

    public boolean shouldUpdate(long now) {
        return now - lastUpdateTime >= 500;
    }

    public boolean shouldUpdate() {
        return shouldUpdate(System.currentTimeMillis());
    }

    public long row() {
        return row;
    }

    public int percent() {
        return rowCount > 0 ? (int)(100 * row / rowCount) : 100;
    }

    public void print(Terminal terminal) {
        var as = toAttributedStringBuilder().toAttributedString();
        int columns = as.columnLength();
        int terminalColumns = terminal.getColumns();
        if (terminalColumns > 0 && columns >= terminalColumns) {
            as = as.columnSplitLength(terminalColumns - 3).getFirst();
            as = new AttributedStringBuilder().append(as).append("...").toAttributedString();
        }
        long now = System.currentTimeMillis();
        terminal.beginSynchronizedUpdate();
        try {
            if (terminal.getStringCapability(InfoCmp.Capability.clr_eol) != null) {
                terminal.puts(InfoCmp.Capability.carriage_return);
                terminal.puts(InfoCmp.Capability.clr_eol);
                as.print(terminal);
            } else {
                as.println(terminal);
            }
        } finally {
            terminal.endSynchronizedUpdate();
            terminal.flush();
            lastUpdateTime = now;
        }
    }

    public AttributedStringBuilder toAttributedStringBuilder() {
        var b = new AttributedStringBuilder()
            .styled(faintStyle, "[" + stopwatch + "]")
            .append(' ')
            .append(String.format("[%3d%%]", percent()))
            .append(' ')
            .styled(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN).underline(), name);
        if (row > 0) {
            double sec = stopwatch.toSeconds();
            b.append(' ').styled(faintStyle, String.format(rowCountFormat2, row, rowCount, row / sec));
        } else {
            b.append(' ').styled(faintStyle, String.format(rowCountFormat, row, rowCount));
        }
        if (stopwatch.isStopped()) {
            b.append(' ').styled(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN), "✓");
        }
        return b;
    }

    public String toString() {
        return new StringBuilder()
            .append('[')
            .append(stopwatch)
            .append("] ")
            .append(name)
            .append(' ')
            .append(String.format(rowCountFormat, row))
            .append('/')
            .append(rowCount)
            .append(" (")
            .append(percent())
            .append("%)")
            .toString();
    }
}
