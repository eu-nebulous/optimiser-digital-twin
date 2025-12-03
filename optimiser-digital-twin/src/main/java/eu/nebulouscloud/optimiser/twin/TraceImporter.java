package eu.nebulouscloud.optimiser.twin;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

@Slf4j
@Command(name = "import-traces",
    description = "Create trace database from server log file",
    mixinStandardHelpOptions = true)
public class TraceImporter implements Callable<Integer> {

    @ParentCommand
    private Main app;

    @Parameters(index = "0", description = "The database file to be created")
    private Path dbFile;

    @Parameters(index = "1", description = "A file containing trace logs")
    private Path traceFile;

    @Override
    public Integer call() {
        try {
            storeLog(dbFile, traceFile);
        } catch (SQLException e) {
            log.error("Database error during trace import", e);
            return 1;
        } catch (IOException e) {
            log.error("File error during trace import", e);
            return 2;
        }
        return 0;
    }

    /**
     * Check if {@code event} has all required attributes of a well-formed
     * trace event.
     */
    public static boolean isEventWellformed(JsonNode event) {
        Set<String> requiredKeys = Set.of("CompName", "ReplicaID",
            "EventType", "EventTime", "PayloadSize", "ActivityID",
            "RemoteCompName");
        for (String key : requiredKeys) {
            if (event.get(key) == null) return false;
        }
        if (!(event.at("/EventTime").canConvertToLong())) return false;
        if (!(event.at("/PayloadSize").canConvertToLong())) return false;
        return true;
    }

    /**
     * Read a log file and return a sequence of LogEntry records containing
     * all well-formed log entry lines in the file.
     *
     * @throws IOException if the file cannot be read
     */
    public static List<LogEntry> readLog(Path file) throws IOException {
        return readLog(Files.lines(file).iterator());
    }

    /**
     * Process a sequence of log output lines and return a sequence of JSON
     * objects containing all well-formed trace entries.  Log entries are
     * well-formed if they are parseable as JSON and contain all required
     * attributes.
     */
    public static List<LogEntry> readLog(Iterator<String> logLines) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayList<LogEntry> log = new ArrayList<>();
        while (logLines.hasNext()) {
            String line = logLines.next();
            JsonNode event;
            try {
                event = mapper.readTree(line);
            } catch (JsonProcessingException e) {
                // random log line that's not in json format
                continue;
            }
            if (!isEventWellformed(event)) {
                // random json log line that's not a trace event
                continue;
            } else {
                log.addLast(new LogEntry(
                    event.at("/CompName").asText(),
                    event.at("/ReplicaID").asText(),
                    event.at("/RemoteCompName").asText(),
                    LogEntry.LogEntryType.fromString(event.at("/EventType").asText()),
                    event.at("/ActivityID").asText(),
                    event.at("/EventTime").asLong(),
                    event.at("/PayloadSize").asLong()));
            }
        }
        return log;
    }

    /**
     * Import traces from a file into the default database.
     * @throws IOException 
     */
    public static long storeLog(Path traceFile) throws IOException {
        log.info("Note: default database location not implemented yet");
        Iterator<String> lines = Files.lines(traceFile).iterator();
        List<LogEntry> entries = readLog(lines);
        return entries.size();
    }

    /**
     * Import traces from a file into a database.
     *
     * @param dbFile the database to import into.
     * @param traceFile the file containing log entries in JSONL format.
     * @return the number of trace events imported.
     */
    public static long storeLog(Path dbFile, Path traceFile) throws IOException, SQLException {
        log.info("Reading log {} into database {}", traceFile, dbFile);
        Iterator<String> lines = Files.lines(traceFile).iterator();
        return storeLog(dbFile, readLog(lines));
    }

    /**
     * Import traces into a database.
     */
    public static long storeLog(Path dbFile, BufferedReader traceFile) throws IOException, SQLException {
        Iterator<String> lines = traceFile.lines().iterator();
        return storeLog(dbFile, readLog(lines));
    }

    /**
     * Import traces in JSONL format into a database.
     *
     * @param dbFile the database to import into.
     * @param logLines an iterator producing lines of log entries in jsonl format
     * @return the number of trace events imported
     * @throws SQLException
     * @throws IOException
     */
    public static long storeLog(Path dbFile, List<LogEntry> events) throws SQLException, IOException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS trace_events(
                  local_name STRING,
                  local_id STRING,
                  remote_name STRING,
                  activity_id STRING,
                  event_type STRING,
                  event_time INTEGER,
                  payload_size INTEGER)
                """);
            try (PreparedStatement insert = connection.prepareStatement("""
                     INSERT INTO trace_events (local_name, local_id, remote_name, activity_id,
                       event_type, event_time, payload_size)
                     VALUES (?, ?, ?, ?,
                       ?, MAX(0, ?), MAX(0, ?))
                     """)) {
                long nInserts = 0;
                for (LogEntry event : events) {
                    insert.setString(1, event.CompName());
                    insert.setString(2, event.ReplicaID());
                    insert.setString(3, event.RemoteCompName());
                    insert.setString(4, event.ActivityID());
                    insert.setString(5, event.EventType().toString());
                    insert.setLong(6, event.EventTime());
                    insert.setLong(7, event.PayloadSize());
                    insert.addBatch();
                    nInserts = nInserts + 1;
                    if (nInserts % 10000 == 0) insert.executeBatch();
                }
                insert.executeBatch();
                return nInserts;
            }
        }
    }
}
