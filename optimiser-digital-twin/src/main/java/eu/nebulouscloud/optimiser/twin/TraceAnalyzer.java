package eu.nebulouscloud.optimiser.twin;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

@Slf4j
@Command(name = "analyze-traces",
    description = "Analyze real and simulation trace log files",
    mixinStandardHelpOptions = true)
public class TraceAnalyzer implements Callable<Integer> {

    @ParentCommand
    private Main app;

    @Parameters(description = "The trace logs to be analyzed")
    private final List<Path> logFiles = List.of();

    @Override
    public Integer call() {
        logFiles.forEach(file -> {
            try {
                final List<LogEntry> entries = TraceImporter.readLog(file);
                printTraceStatistics(entries);
            } catch (final IOException e) {
                log.error("Could not read file " + file, e);
            }
        });
        return 0;
    }

    /**
     * Given a list of log entries all belonging to the same component, return
     * the list of processing times for events.  The times are calculated as
     * the difference between each "in" event and subsequent "out" events, for
     * all events having the same Replica ID.
     */
    private static List<Long> calculateTimesForComponent(final List<LogEntry> entries) {
        final List<Long> result = new ArrayList<>();
        final Map<String, Long> lastInEventTimePerActivity = new HashMap<>();
        final List<LogEntry> sortedEntries = entries.stream()
            .sorted(Comparator.comparing((e) -> e.EventTime()))
            .toList();
        for (final LogEntry entry : sortedEntries) {
            final long t = entry.EventTime();
            final String activityId = entry.ActivityID();
            switch (entry.EventType()) {
                case IN:
                    lastInEventTimePerActivity.put(activityId, t);
                    break;
                case OUT:
                    if (lastInEventTimePerActivity.containsKey(activityId)) {
                        result.add(t - lastInEventTimePerActivity.get(activityId));
                    }
                    break;
                default:
                    // not handling "ack" events
                    break;
            }
        }
        return result;
    }

    /**
     * Given a list of log entries, return a list of processing times per
     * component.  The keys in the returned map are of the form {@code
     * CompName + "|" + ReplicaID}.
     */
    static Map<String, List<Long>> calculateProcessingTimesPerComponent(final List<LogEntry> logEntries) {
        final Map<String, List<LogEntry>> eventsByComponent = logEntries.stream()
            .collect(Collectors.groupingBy((event) -> event.CompName()
                                                      + "|" + event.ReplicaID()));
        final Map<String, List<Long>> eventTimesPerComponent = eventsByComponent.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> calculateTimesForComponent(entry.getValue())));
        return eventTimesPerComponent;
    }

    /**
     * Calculate statistics for the event times of each activity in a stream.
     */
    static Map<String, LongSummaryStatistics> calculateTimePerActivity(final List<LogEntry> trace) {
        return trace.stream()
            .collect(Collectors.groupingBy(
                LogEntry::ActivityID,
                Collectors.summarizingLong(LogEntry::EventTime)));
    }

    /**
     * Print out statistics for the trace; currently human-readable and
     * unordered.
     */
    private static void printTraceStatistics(final List<LogEntry> entries) {
        System.out.format("Valid trace entries: %d%n%n", entries.size());

        final var statsPerActivity = calculateTimePerActivity(entries);
        final var overallDurationStats = statsPerActivity.values().stream()
            .collect(Collectors.summarizingLong((s) -> s.getMax() - s.getMin()));

        System.out.format("Activity statistics: count: %s, execution time min: %s, max: %s, average: %s%n",
            overallDurationStats.getCount(), overallDurationStats.getMin(), overallDurationStats.getMax(), overallDurationStats.getAverage());
        // Use Jackson to generate CSV outpout: this is many more lines of
        // code but will handle string escaping of funky component names
        CsvSchema schema = CsvSchema.builder()
            .addColumn("ActivityID")
            .addColumn("executionTime")
            .setUseHeader(true)
            .build();
        StringWriter collector = new StringWriter();
        try (var csvWriter = new CsvMapper().writer(schema).writeValues(collector)) {
            for (var e : statsPerActivity.entrySet()) {
                csvWriter.write(new Object[]{ e.getKey(), e.getValue().getMax() - e.getValue().getMin() } );
            }
        } catch (IOException e) {
            // Shouldn't happen
            log.error("Failed to print activity statistics", e);
        }
        System.out.println(collector.toString());
        System.out.println();

        final Map<String, List<Long>> times = calculateProcessingTimesPerComponent(entries);
        System.out.format("Components: %d%n", times.size());
        times.forEach((component, compTimes) -> {
            System.out.format("Times for \"%s\": %s%n", component, compTimes);
        });
    }

}
