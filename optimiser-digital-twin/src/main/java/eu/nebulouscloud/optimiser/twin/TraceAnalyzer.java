package eu.nebulouscloud.optimiser.twin;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

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
    private List<Path> logFiles = List.of();

    @Override
    public Integer call() {
        logFiles.forEach(file -> {
            try {
                List<LogEntry> entries = TraceImporter.readLog(file);
                printTraceStatistics(entries);
            } catch (IOException e) {
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
    private static List<Long> calculateTimesForComponent(List<LogEntry> entries) {
        List<Long> result = new ArrayList<>();
        Map<String, Long> lastInEventTimePerActivity = new HashMap<>();
        List<LogEntry> sortedEntries = entries.stream()
            .sorted(Comparator.comparing((e) -> e.EventTime()))
            .toList();
        for (LogEntry entry : sortedEntries) {
            long t = entry.EventTime();
            String activityId = entry.ActivityID();
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
    static Map<String, List<Long>> calculateProcessingTimes(List<LogEntry> logEntries) {
        Map<String, List<LogEntry>> eventsByComponent = logEntries.stream()
            .collect(Collectors.groupingBy((event) -> event.CompName()
                                                      + "|" + event.ReplicaID()));
        Map<String, List<Long>> eventTimesPerComponent = eventsByComponent.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                entry -> calculateTimesForComponent(entry.getValue())));
        return eventTimesPerComponent;
    }

    private static void printTraceStatistics(List<LogEntry> entries) {
        System.out.format("Valid trace entries for %d%n", entries.size());

        Map<String, List<Long>> times = calculateProcessingTimes(entries);
        times.forEach((component, compTimes) -> {
            System.out.format("Times for component %s: %s%n", component, compTimes);
        });
    }

}
