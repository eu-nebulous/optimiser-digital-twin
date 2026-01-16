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
import java.util.Set;
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

    private enum ComponentType {
        Unknown,       // placeholder
        Source,        // Component only logs OUT events
        Active,        // component logs IN and OUT events
        Passive        // no own log, active components log OUT and ACK events
    }

    /**
     * Print out statistics for the trace; currently human-readable and
     * unordered.
     */
    private static void printTraceStatistics(final List<LogEntry> entries) {
        // Note that we use Jackson to generate CSV output: this is many more
        // lines of code but will produce valid CSV even with crazy component
        // or event names

        // Trace statistics
        System.out.format("Trace events: %d%n%n", entries.size());

        // Activity statistics
        final Map<String, LongSummaryStatistics> statsPerActivity = calculateTimePerActivity(entries);
        final LongSummaryStatistics overallDurationStats = statsPerActivity.values().stream()
            .collect(Collectors.summarizingLong((s) -> s.getMax() - s.getMin()));

        System.out.format("Activity statistics: count: %s, execution time min: %s, max: %s, average: %s%n",
            overallDurationStats.getCount(), overallDurationStats.getMin(), overallDurationStats.getMax(), overallDurationStats.getAverage());

        final CsvSchema activitySchema = CsvSchema.builder()
            .addColumn("ActivityID")
            .addColumn("executionTime")
            .setUseHeader(true)
            .build();
        final StringWriter activityCollector = new StringWriter();
        try (var csvWriter = new CsvMapper().writer(activitySchema).writeValues(activityCollector)) {
            for (final var e : statsPerActivity.entrySet()) {
                csvWriter.write(new Object[]{ e.getKey(), e.getValue().getMax() - e.getValue().getMin() } );
            }
        } catch (final IOException e) {
            // Shouldn't happen
            log.error("Failed to print activity statistics", e);
        }
        System.out.println(activityCollector.toString());
        System.out.println();

        // Component statistics
        final Map<String, ComponentType> components = classifyComponents(entries);
        final Map<String, Set<String>> componentReplicas = componentReplicas(entries);
        System.out.println("Component types");
        System.out.println("Component,type,replica_ids");
        components.forEach((name, type) -> System.out.format("%s,%s,%s%n",
            name, type, componentReplicas.getOrDefault(name, Set.of())));
        System.out.println();

        final Map<String, Map<String, List<Long>>> componentTimes = calculateProcessingTimesPerReplica(components, componentReplicas, entries);
        final CsvSchema componentSchema = CsvSchema.builder()
            .addColumn("Component")
            .addColumn("replica_id")
            .addColumn("task_count")
            .addColumn("min")
            .addColumn("max")
            .addColumn("average")
            .setUseHeader(true)
            .build();
        final StringWriter componentCollector = new StringWriter();
        try (var csvWriter = new CsvMapper().writer(componentSchema).writeValues(componentCollector)) {
            componentTimes.forEach((component, entry) -> {
                entry.forEach((replica, times) -> {
                    LongSummaryStatistics stats = times.stream().mapToLong(Long::longValue).summaryStatistics();
                    try {
                        csvWriter.write(new Object[] {
                            component,
                            replica,
                            stats.getCount(),
                            stats.getMin(),
                            stats.getMax(),
                            stats.getAverage() } );
                    } catch (IOException e) {
                        // Shouldn't happen
                        log.error("Failed to print component task statistics", e);
                    }
                });
            });
        } catch (final IOException e) {
            // Shouldn't happen
            log.error("Failed to print component statistics", e);
        }
        System.out.println(componentCollector.toString());
        System.out.println();

        System.out.format("Non-Source replicas: %d%n", componentTimes.size());
        System.out.println("Component,replica_id,times");
        componentTimes.forEach((component, compTimes) -> {
            compTimes.forEach((replica, times) -> {
                System.out.format("%s,%s,%s%n", component, replica, times);
            });
        });
    }

    /**
     * Classify components according to what they log in the trace.  If we
     * find an IN event, the component is active (records its own incoming
     * events); otherwise, we see an OUT event, it is a source components
     * (representing input to the system); otherwise, if we see the component
     * as the remote partner of an ACK event, the component is passive.
     */
    private static Map<String, ComponentType> classifyComponents(final List<LogEntry> entries) {
        final Map<String, ComponentType> result = new HashMap<>();
        entries.forEach(e -> {
            switch (e.EventType()) {
                case IN: result.put(e.CompName(), ComponentType.Active); break;
                case OUT: result.putIfAbsent(e.CompName(), ComponentType.Source); break;
                // Note: we trust that the trace is well-formed in that no
                // component logs both its own events *and* has another
                // component logging ack.  Nevertheless, if it happens, we
                // treat the component as active.
                case ACK: result.putIfAbsent(e.RemoteCompName(), ComponentType.Passive); break;
                case UNKNOWN: break;
            }
        });
        return result;
    }

    /**
     * Return a list of replica ids per component.
     */
    private static Map<String, Set<String>> componentReplicas(final List<LogEntry> entries) {
        return entries.stream()
            .collect(Collectors.groupingBy(LogEntry::CompName,
                Collectors.mapping(LogEntry::ReplicaID, Collectors.toSet())));
    }

    /**
     * Given a list of log entries, return a list of processing times per
     * component.  The keys in the returned map are of the form {@code
     * CompName + "|" + ReplicaID}.
     * @param componentTypes All components and their type.
     * @param componentReplicas List of replica IDs for each component
     * @param logEntries all parsed log events, not necessarily sorted.
     * @return a map from Component -> Replica -> list of processing times
     */
    private static Map<String, Map<String, List<Long>>> calculateProcessingTimesPerReplica(
        final Map<String, ComponentType> componentTypes,
        Map<String, Set<String>> componentReplicas,
        final List<LogEntry> logEntries)
    {
        final Map<String, Map<String, List<Long>>> result = new HashMap<>();
        componentTypes.forEach((name, type) -> {
            switch (type) {
                case ComponentType.Source: break; // could also record empty list for each replica
                case ComponentType.Active: {
                    // Collect task times for each replica
                    componentReplicas.getOrDefault(name, Set.of("")).forEach(
                        replica -> {
                            List<Long> taskTimes = calculateTimesForComponent(type,
                                logEntries.stream()
                                    .filter(e -> e.CompName().equals(name) && e.ReplicaID().equals(replica))
                                    .toList());
                            result.putIfAbsent(name, new HashMap<>());
                            result.get(name).put(replica, taskTimes);
                        }
                    );
                }
                    break;
                case ComponentType.Passive: {
                    // Collect task times for component (no replica id available)
                    List<Long> taskTimes = calculateTimesForComponent(type,
                        logEntries.stream()
                            .filter(e -> e.RemoteCompName().equals(name))
                            .toList());
                    result.putIfAbsent(name, new HashMap<>());
                    result.get(name).put("", taskTimes);
                }
                    break;
                case ComponentType.Unknown: break; // should never happen
            }
        });
        return result;
    }

    /**
     * Given a list of log entries all belonging to the same component and
     * replica, return the list of processing times for events belonging to
     * the same activity.
     *
     * For active components, the times are calculated as the difference
     * between each "in" event and subsequent "out" events, for all events
     * having the same Replica ID.  For passive components, the times are
     * calculated as the difference between "out" events and "ack" events.
     */
    private static List<Long> calculateTimesForComponent(ComponentType type, final List<LogEntry> entries) {
        final List<Long> result = new ArrayList<>();
        final Map<String, Long> lastInEventTimePerActivity = new HashMap<>();
        final List<LogEntry> sortedEntries = entries.stream()
            .sorted(Comparator.comparing((e) -> e.EventTime()))
            .toList();
        LogEntry.LogEntryType startType = type == ComponentType.Active ? LogEntry.LogEntryType.IN : LogEntry.LogEntryType.OUT;
        LogEntry.LogEntryType endType = type == ComponentType.Active ? LogEntry.LogEntryType.OUT : LogEntry.LogEntryType.ACK;
        for (final LogEntry entry : sortedEntries) {
            final long t = entry.EventTime();
            final String activityId = entry.ActivityID();
            if (entry.EventType().equals(startType)) {
                // note the start time of an activity
                lastInEventTimePerActivity.put(activityId, t);
            } else if (entry.EventType().equals(endType)) {
                if (lastInEventTimePerActivity.containsKey(activityId)) {
                    // record end of activity (note we can have more than one
                    // end for one start; in that case, we decide to record
                    // all outgoing events as separate activities.)
                    result.add(t - lastInEventTimePerActivity.get(activityId));
                }
            }
        }
        return result;
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

}
