package eu.nebulouscloud.optimiser.twin;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import org.slf4j.MDC;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import groovyjarjarpicocli.CommandLine.ParentCommand;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Slf4j
@Command(name = "import-deployment",
    description = "Create deployment database from solver solution and application message",
    mixinStandardHelpOptions = true)
public class DeploymentImporter implements Callable<Integer> {
    @ParentCommand
    private Main app;

    @Parameters(index = "0", description = "The database file to be created")
    private Path dbFile;

    @ArgGroup(exclusive=true, multiplicity="1")
    Input input;

    static class Input {
        @Option(names = { "-m", "--app-creation-message"}, description = "A file containing the (Nebulous) app creation message")
        private Optional<Path> appCreationMessageFile;

        @Option(names = { "-d", "--deployment-file" }, description = "A CSV files containing a deployment scenario")
        private Optional<Path> deploymentScenarioFile;
    }

    @Option(names = { "-s", "--solution" }, description = "A file containing a solver message with machine configurations")
    private Optional<Path> solverSolutionFile;

    // @JsonPropertyOrder({ "Component", "Replicas", "Cores", "Memory"})
    private record CsvEntry(String Component, int Replicas, int Cores, int Memory) {};

    @Override
    public Integer call() {
        ObjectMapper mapper = new ObjectMapper();
        NebulousApp app = null;
        JsonNode appMessage = null;
        JsonNode solverSolution = null;
        if (input.appCreationMessageFile.isPresent()) {
            try {
                appMessage = mapper.readTree(Files.readString(input.appCreationMessageFile.get(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                log.error("Could not read app creation message file: {}", input.appCreationMessageFile.get());
                return 1;
            }
            try {
                app = NebulousApp.fromAppMessage(appMessage);
            } catch (JsonProcessingException e) {
                log.error("Could not parse app creation message", e);
                return 1;
            }
            try {
                if (solverSolutionFile.isPresent()) {
                    solverSolution = mapper.readTree(Files.readString(solverSolutionFile.get(),
                        StandardCharsets.UTF_8));
                }
            } catch (IOException e) {
                log.error("Could not read solver solution file: {}", solverSolutionFile);
            }
            boolean success = saveSolverSolution(dbFile, app, solverSolution);
            return success ? 0 : 1;
        } else if (input.deploymentScenarioFile.isPresent()) {
            if (solverSolutionFile.isPresent()) {
                log.warn("Solver solutions only apply to app creation messages; ignoring {} during import of CSV deployment scenario",
                    solverSolutionFile.get());
            }
            try {
                String csv = Files.readString(input.deploymentScenarioFile.get());
                boolean success = saveCsvScenario(dbFile, csv);
                return success ? 0 : 1;
            } catch (IOException e) {
                log.error("Could not create database file", e);
                return 1;
            }
        } else {
            // should be impossible
            log.error("Need either app creation message or cvs scenario; aborting");
            return 1;
        }
    }

    /**
     * Create a database with the nodes specified by the given CSV data.
     *
     * @param dbName the name of the database to be created
     * @param csvString the CSV data as a string
     * @return true if the database creation was successful, false otherwise.
     */
    public static boolean saveCsvScenario(Path dbName, String csvString) {
        CsvMapper csvMapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema()
            .withHeader();

        try (Connection connection = initDatabase(dbName);
             PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO scenario (component, cpu, memory, replicas)
                    VALUES (?, ?, ?, ?)
                    """))
        {
            MappingIterator<CsvEntry> it = csvMapper
                .readerFor(CsvEntry.class)
                .with(schema)
                .readValues(csvString);
            for (CsvEntry entry : it.readAll()) {
                insert.setString(1, entry.Component);
                insert.setInt(2, entry.Cores);
                insert.setInt(3, entry.Memory);
                insert.setInt(4, entry.Replicas);
                insert.addBatch();
            }
            int[] results = insert.executeBatch();
            log.info("Created {} entries in database {}", results.length, dbName);
        } catch (SQLException | IOException e) {
            log.error("Could not create database", e);
            return false;
        }
        return true;
    }

    /** Location of the deploy flag in the solver solution message */
    private static final JsonPointer deployPath = JsonPointer.compile("/DeploySolution");
    /** Location of the solution variables in the solver solution message */
    private static final JsonPointer variablesPath = JsonPointer.compile("/VariableValues");


    /**
     * Create a database with the nodes specified by the app's kubevela,
     * optionally modified by the given solution.
     *
     * @param dbName the name of the database to be created
     * @param app the NebulousApp
     * @param solution a solver solution or null
     * @return true if the database creation was successful, false otherwise.
     */
    public static boolean saveSolverSolution(Path dbName, NebulousApp app, JsonNode solution) {
        JsonNode effectiveKubevela = app.getKubevela();
        if (solution != null) {
            JsonNode solutionId = solution.at("/application");
            if (solutionId.isMissingNode()) {
                log.error("Solver solution does not contain 'application' property, aborting");
                return false;
            }
            if (!solutionId.asText().equals(app.getUuid())) {
                log.error("Tried to apply solver solution for app id {} to application with app id {}, aborting",
                    solutionId.asText(), app.getUuid());
            }
            ObjectNode variableValues = solution.withObject(variablesPath);
            effectiveKubevela = app.rewriteKubevelaWithSolution(variableValues);
        }
        Map<String, Integer> replicas = KubevelaAnalyzer.getNodeCount(effectiveKubevela);

        try (Connection connection = initDatabase(dbName);
             PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO scenario (component, cpu, memory, replicas)
                    VALUES (?, ?, ?, ?)
                    """))
        {
            MDC.pushByKey("appId", app.getUuid());
                for (JsonNode c : KubevelaAnalyzer.getNodeComponents(effectiveKubevela).values()) {
                    String componentName = c.at("/name").asText();
                    long cpu = KubevelaAnalyzer.getCpuRequirement(c, componentName);
                    long memory = KubevelaAnalyzer.getMemoryRequirement(c, componentName);
                    int nreplicas = replicas.getOrDefault(componentName, 1);
                    insert.setString(1, componentName);
                    insert.setLong(2, cpu);
                    insert.setLong(3, memory);
                    insert.setLong(4, nreplicas);
                    insert.addBatch();
                }
                int[] results = insert.executeBatch();
                log.info("Created {} entries in database {}", results.length, dbName);
        } catch (SQLException e) {
            log.error("Could not create database", e);
            return false;
        } finally {
            MDC.popByKey("appId");
        }
        return true;
    }

    /**
     * Initialize the database and table.  Note that the returned {@code
     * Connection} object needs to be closed.
     *
     * @param dbName the database to be created
     * @return the database connection
     * @throws SQLException if creating the database or table failed
     */
    private static Connection initDatabase(Path dbName) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbName);
        Statement statement = connection.createStatement();
        statement.executeUpdate("DROP TABLE IF EXISTS scenario");
        statement.executeUpdate("""
                CREATE TABLE scenario (
                  component STRING,
                  cpu NUMBER,
                  memory NUMBER,
                  replicas NUMBER)
                """);
        statement.close();
        return connection;
    }
}
