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
import java.util.concurrent.Callable;

import org.slf4j.MDC;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import groovyjarjarpicocli.CommandLine.ParentCommand;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Slf4j
@Command(name = "import-deployment",
    description = "Create deployment database from solver solution and application message",
    mixinStandardHelpOptions = true)
public class DeploymentImporter implements Callable<Integer> {
    @ParentCommand
    private Main app;

    @Parameters(description = "A file containing the app creation message")
    private Path appCreationMessage;

    @Parameters(description = "A file containing a solver message with machine configurations")
    private Path configurationFile;

    @Parameters(description = "The database file to be created")
    private Path dbFile = Path.of("config.db");

    @Override
    public Integer call() {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode message = null;
        try {
            message = mapper.readTree(Files.readString(configurationFile, StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.error("Could not read solver solution file: {}", configurationFile);
            return 1;
        }
        boolean success = saveSolverSolution(message, dbFile);
        return success ? 0 : 1;
    }



    private static final String DEPLOY_PROPERTY = "DeploySolution";
    private static final String VARIABLEVALUES_PROPERTY = "VariableValues";


    public static boolean saveSolverSolution(JsonNode solution, Path dbName) {
        if (solution.at("/application").isMissingNode()) {
            log.error("Solver solution does not contain 'application' property, aborting");
            return false;
        }
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbName);
             Statement statement = connection.createStatement();
             PreparedStatement insert = connection.prepareStatement("INSERT INTO scenario(component, cpu, memory) VALUES (?, ?, ?)");
        ) {
            String app_id = solution.at("/application").toString(); // should be string already, but don't want to cast
            MDC.pushByKey("appId", app_id);

            if (!solution.has(DEPLOY_PROPERTY)) {
                log.warn("Received solver solution without DeploySolution field, ignoring.");
                return false;
            }
            if (!solution.at("/DeploySolution").asBoolean(false)) {
                // `asBoolean` returns its argument if node is missing or cannot
                // be converted to Boolean
                log.info("Received solver solution with DeploySolution=false, ignoring.");
                return false;
            }
            statement.executeUpdate("DROP TABLE IF EXISTS scenario");

            ObjectNode variableValues = solution.withObjectProperty(VARIABLEVALUES_PROPERTY);
            for (Map.Entry<String, JsonNode> entry : variableValues.properties()) {
                String key = entry.getKey();
                JsonNode replacementValue = entry.getValue();
                // TODO: parse app creation message first; find component name
                // etc. there
                insert.setString(1, "foo");
                insert.setLong(2, 1);
                insert.setLong(3, 1);
                insert.addBatch();
            }
            int[] results = insert.executeBatch();
            log.info("Created {} entries in database {}", results.length, dbName);
        } catch (SQLException e) {
            log.error("Could not open database {}", dbName);
            return false;
	} finally {
            MDC.popByKey("appId");
        }
        return true;
    }
}
