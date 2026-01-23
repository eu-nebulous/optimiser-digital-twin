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
        try (BufferedReader lines = Files.newBufferedReader(file)) {
            return readLog(lines);
        }
    }

    /**
     * Process a sequence of log output lines and return a sequence of JSON
     * objects containing all well-formed trace entries.  Log entries are
     * well-formed if they are parseable as JSON and contain all required
     * attributes.
     *
     * @param logLines stream of lines; will not be closed by this method.
     * @throws IOException 
     */
    public static List<LogEntry> readLog(BufferedReader logLines) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ArrayList<LogEntry> log = new ArrayList<>();
        String line;
        while ((line = logLines.readLine()) != null) {
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
     */
    public static long storeLog(Path traceFile) throws IOException {
        // TODO implement
        log.error("Note: default database location not implemented yet");
        return -1;
    }

    /**
     * Import traces in JSONL format into a database.  Note that this method
     * does not clear the database before importing.
     *
     * @param dbFile the database to import into
     * @param traceFile the file containing log entries in JSONL format.
     * @return the number of trace events imported
     * @throws SQLException
     * @throws IOException
     */
    public static long storeLog(Path dbFile, Path traceFile) throws IOException, SQLException {
        log.info("Reading log {} into database {}", traceFile, dbFile);
        try (BufferedReader lines = Files.newBufferedReader(traceFile)) {
            return storeLog(dbFile, lines);
        }
    }

    /**
     * Import traces in JSONL format into a database.  This method exists
     * mostly for the benefit of our unit tests.  {@see
     * TraceImporter#storeLog(Path, Path))}.
     */
    public static long storeLog(Path dbFile, BufferedReader lines) throws IOException, SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile);
             Statement statement = connection.createStatement())
        {
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
                     """))
            {
                // Perform inserts in a transaction: reduces execution time
                // from 25min to 5s for 235MB of logs
                connection.setAutoCommit(false);
                long nInserts = 0;
                String line;
                ObjectMapper mapper = new ObjectMapper();
                while ((line = lines.readLine()) != null) {
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
                        insert.setString(1, event.at("/CompName").asText());
                        insert.setString(2, event.at("/ReplicaID").asText());
                        insert.setString(3, event.at("/RemoteCompName").asText());
                        insert.setString(4, event.at("/ActivityID").asText());
                        insert.setString(5, event.at("/EventType").asText());
                        insert.setLong(6, event.at("/EventTime").asLong());
                        insert.setLong(7, event.at("/PayloadSize").asLong());
                        insert.addBatch();
                        nInserts = nInserts + 1;
                        if (nInserts % 10000 == 0) insert.executeBatch();
                    }
                }
                insert.executeBatch();
                connection.commit();
                return nInserts;
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        }
    }
}
