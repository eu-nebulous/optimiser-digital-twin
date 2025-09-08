package eu.nebulouscloud.optimiser.twin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.Callable;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

@Slf4j
@Command(name = "simulate",
    description = "Execute one simulation run",
    mixinStandardHelpOptions = true)
public class Simulator implements Callable<Integer> {

    @Parameters(description = "The trace database")
    private Path traceDbParam;

    @Parameters(description = "The deployment scenario database")
    private Path scenarioDbParam;

    @ParentCommand
    private Main app;

    @Override
    public Integer call() throws Exception {
        String result = simulate(traceDbParam, scenarioDbParam);
        if (result != null) {
            System.out.println(result);
            return 0;
        } else {
            return 1;
        }
    }

    /**
     * Run a simulation, and return the output as a string.
     *
     * <p>This method has the following global side effects while executing:
     * we set the {@code abs.datadir} property to a temporary directory
     * containing the data files, and we set {@code System.out} to a stream
     * that records the output of the model.
     *
     * @param traceDb a database file containing traces.
     * @param scenarioDb a database file containing the deployment scenario.
     * @return the model's output (unparsed), or null if the simulation failed.
     */
    public static String simulate(Path traceDb, Path scenarioDb) {
        if (!traceDb.toFile().canRead()) {
            log.error("File {} does not exist or cannot be read", traceDb);
            return null;
        }
        if (!scenarioDb.toFile().canRead()) {
            log.error("File {} does not exist or cannot be read", scenarioDb);
            return null;
        }

        Path tempDir = null;
        String origAbsDatadir = System.getProperty("abs.datadir");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        PrintStream originalOut = System.out;
        try {
            tempDir = Files.createTempDirectory("simulation-");
            System.setProperty("abs.datadir", tempDir.toAbsolutePath().toString());
            tempDir.toFile().deleteOnExit(); // note that this only deletes when the VM exists regularly
            Files.copy(traceDb, tempDir.resolve("trace.db"));
            Files.copy(scenarioDb, tempDir.resolve("scenario.db"));
            System.setOut(printStream);
            Twin.Main.main(new String[0]);
        } catch (Exception e) {
            log.error("Simulation failed", e);
            return null;
        } finally {
            System.setOut(originalOut);
            if (origAbsDatadir == null) {
                System.clearProperty("abs.datadir");
            } else {
                System.setProperty("abs.datadir", origAbsDatadir);
            }
        }

        if (tempDir != null) {
            // best-effort deletion of the temporary directory
            try {
                Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try { Files.delete(path); }
                        catch (IOException e) { /* ignore */ }
                });
            } catch (IOException e) { /* ignore */ }
        }

        return outputStream.toString();
    }

}
