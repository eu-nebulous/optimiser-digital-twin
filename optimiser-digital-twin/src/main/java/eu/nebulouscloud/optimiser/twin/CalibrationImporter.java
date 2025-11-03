package eu.nebulouscloud.optimiser.twin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.MDC;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import groovyjarjarpicocli.CommandLine.ParentCommand;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Slf4j
@Command(name = "import-calibration",
    description = "Create calibration parameter database from calibration values",
    mixinStandardHelpOptions = true)
public class CalibrationImporter implements Callable<Integer> {
    @ParentCommand
    private Main app;

    @Parameters(index = "0", description = "The database file to be created")
    private Path dbFile;

    @Parameters(index = "1", description = "A file containing the calibration parameters as CSV")
    private Path calibrationCsvFile;

    @Override
    public Integer call() {
	try {
	    String calibrationCsv = Files.readString(calibrationCsvFile);
            boolean success = saveCalibration(dbFile, calibrationCsv);
            return success ? 0 : 1;
	} catch (IOException e) {
            log.error("Could not create database file", e);
            return 1;
	}
    }

    @JsonPropertyOrder({ "component", "constant_factor", "variable_factor" })
    private static class CalibrationEntry {
        public String component;
        public double constant_factor;
        public double variable_factor;
    }

    /**
     * Create a database with the calibration values.
     *
     * @param dbName the name of the database to be created
     *
     * @param a CSV string containing the calibration values.  The CSV
     * contains no header.  Each line is of the format {@code
     * componentName,constantFactor,variableFactor}.
     *
     * @return true if the database creation was successful, false otherwise.
     */
    public static boolean saveCalibration(Path dbName, String csvCalibrationDoc) {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbName);
             Statement statement = connection.createStatement()) {

            statement.executeUpdate("DROP TABLE IF EXISTS factors");
            statement.executeUpdate("""
                CREATE TABLE factors (
                  component STRING,
                  constant_cost REAL,
                  variable_cost REAL)
                """);

            CsvMapper csvMapper = new CsvMapper();
            CsvSchema schema = CsvSchema.builder()
                .addColumn("component")
                .addColumn("constant_factor")
                .addColumn("variable_factor")
                .build();

            try (PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO factors (component, constant_cost, variable_cost)
                    VALUES (?, ?, ?)
                    """)) {
                MappingIterator<CalibrationEntry> it = csvMapper
                    .readerFor(CalibrationEntry.class)
                    .with(schema)
                    .readValues(csvCalibrationDoc);
                for (CalibrationEntry entry : it.readAll()) {
                    insert.setString(1, entry.component);
                    insert.setDouble(2, entry.constant_factor);
                    insert.setDouble(3, entry.variable_factor);
                    insert.addBatch();
                }
                int[] results = insert.executeBatch();
                log.info("Created {} entries in database {}", results.length, dbName);
            }
        } catch (SQLException |IOException e) {
            log.error("Could not create database", e);
            return false;
        }
        return true;
    }
}
