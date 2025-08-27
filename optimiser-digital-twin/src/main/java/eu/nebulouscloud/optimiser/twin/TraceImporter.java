package eu.nebulouscloud.optimiser.twin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

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
            importTraces(dbFile, traceFile);
        } catch (SQLException e) {
            log.error("Database error during trace import", e);
            return 1;
        } catch (IOException e) {
            log.error("File error during trace import", e);
            return 2;
        }
        return 0;
    }

    /** The set of keys needing to be present in a valid jsonl-formatted log
      * line */
    static Set<String> requiredKeys = Set.of("CompName", "ReplicaID", "EventType", "EventTime",
        "PayloadSize", "ActivityID", "RemoteCompName");

    /**
     * Import traces from a file into a database.
     *
     * @param dbFile the database to import into.
     * @param traceFile the file containing log entries in JSONL format.
     * @return the number of trace events imported.
     */
    public static long importTraces(Path dbFile, Path traceFile) throws IOException, SQLException {
        Iterator<String> lines = Files.lines(traceFile).iterator();
        return importTraces(dbFile, lines);
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
    public static long importTraces(Path dbFile, Iterator<String> logLines) throws SQLException, IOException {
        ObjectMapper mapper = new ObjectMapper();
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
                while (logLines.hasNext()) {
                    String line = logLines.next();
                    logEvent(insert, mapper.readTree(line));
                    nInserts = nInserts + 1;
                    if (nInserts % 10000 == 0) insert.executeBatch();
                }
                insert.executeBatch();
                return nInserts;
            }
        }
    }

    /**
     * Check if event contains all required keys, and add a batch statement to
     * statement if yes.
     */
    private static int logEvent(PreparedStatement statement, JsonNode event) {
        for (String key : requiredKeys) {
            if (event.get(key) == null) return 0;
        }
        try {
            statement.setString(1, event.get("CompName").asText());
            statement.setString(2, event.get("ReplicaID").asText());
            statement.setString(3, event.get("RemoteCompName").asText());
            statement.setString(4, event.get("ActivityID").asText());
            statement.setString(5, event.get("EventType").asText());
            statement.setLong(6, event.get("EventTime").asLong());
            statement.setLong(7, event.get("PayloadSize").asLong());
            statement.addBatch();
        } catch (SQLException e) {
            log.trace("Could not log trace event", e);
            return 0;
        }
        return 1;
    }

}
