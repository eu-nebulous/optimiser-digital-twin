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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import groovyjarjarpicocli.CommandLine.ParentCommand;
import lombok.extern.slf4j.Slf4j;
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

    @Parameters(index = "1", description = "A file containing the app creation message")
    private Path appCreationMessageFile;

    @Option(names = { "-s", "--solution" }, description = "A file containing a solver message with machine configurations")
    private Optional<Path> solverSolutionFile;

    @Override
    public Integer call() {
        ObjectMapper mapper = new ObjectMapper();
        NebulousApp app = null;
        JsonNode appMessage = null;
        JsonNode solverSolution = null;
        try {
            appMessage = mapper.readTree(Files.readString(appCreationMessageFile, StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.error("Could not read app creation message file: {}", appCreationMessageFile);
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
    }


    /** Location of the deploy flag in the solver solution message */
    private static final JsonPointer deployPath = JsonPointer.compile("/DeploySolution");
    /** Location of the solution variables in the solver solution message */
    private static final JsonPointer variablesPath = JsonPointer.compile("/VariableValues");


    /**
     * Create a database with the nodes specified by the app's kubevela, as
     * modified by the given solution if not null.
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
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbName);
             Statement statement = connection.createStatement()) {
            MDC.pushByKey("appId", app.getUuid());

            statement.executeUpdate("DROP TABLE IF EXISTS scenario");
            statement.executeUpdate("""
                CREATE TABLE scenario (
                  component STRING,
                  cpu NUMBER,
                  memory NUMBER,
                  replicas NUMBER)
                """);

            try (PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO scenario (component, cpu, memory, replicas)
                    VALUES (?, ?, ?, ?)
                    """)) {
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
            }
        } catch (SQLException e) {
            log.error("Could not create database", e);
            return false;
        } finally {
            MDC.popByKey("appId");
        }
        return true;
    }
}
