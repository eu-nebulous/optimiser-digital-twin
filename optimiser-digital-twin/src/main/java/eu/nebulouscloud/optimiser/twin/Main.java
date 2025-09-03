/*
 * The main class of the twin component.
 */
package eu.nebulouscloud.optimiser.twin;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.ScopeType;

@Command(name = "nebulous-digital-twin",
    version = "0.1",
    mixinStandardHelpOptions = true,
    sortOptions = false,
    separator = " ",
    showAtFileInUsageHelp = true,
    description = "Evaluate deployment scenarios",
    subcommands = {
        Server.class,
        DeploymentImporter.class,
        TraceImporter.class,
        Simulator.class
    }
)
@Slf4j
public class Main implements Callable<Integer> {

    @Option(names = {"--verbose", "-v"},
            description = "Turn on more verbose logging output. Can be given multiple times. When not given, print only warnings and error messages. With @|underline -v|@, print status messages. With @|underline -vvv|@, print everything.",
        scope = ScopeType.INHERIT)
    private boolean[] verbosity;

    @Option(names = {"--log-dir"},
            description = "Directory where to log incoming and outgoing messages as files. Can also be set via the @|bold LOGDIR|@ variable.",
            paramLabel = "LOGDIR",
            defaultValue = "${LOGDIR}")
    @Getter
    private static Path logDirectory;

    /**
     * PicoCLI execution strategy that uses common initialization.
     */
    private int executionStrategy(final ParseResult parseResult) {
        init();
        return new CommandLine.RunLast().execute(parseResult);
    }

    /**
     * Initialization code shared between this class and any
     * subcommands: set logging level, create log directory and create
     * the ActiveMQ adapter.  Note that we do not start the EXN
     * ActiveMQ middleware, so each main method needs to call
     * `activeMQConnector.start` if needed.
     */
    private void init() {
        // Note: in logback.xml we turn on JSON encoding and set the
        // level to WARN.  Here we override the level.
        final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("eu.nebulouscloud");
        if (!(logger instanceof ch.qos.logback.classic.Logger)) {
            log.info("Cannot set log level: logger not of class ch.qos.logback.classic.Logger");
        } else {
            final ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
            if (verbosity != null) {
                switch (verbosity.length) {
                    case 0: break;
                    case 1: logbackLogger.setLevel(ch.qos.logback.classic.Level.INFO); break;
                    case 2: logbackLogger.setLevel(ch.qos.logback.classic.Level.DEBUG); break;
                    case 3: logbackLogger.setLevel(ch.qos.logback.classic.Level.TRACE); break;
                    default: logbackLogger.setLevel(ch.qos.logback.classic.Level.TRACE); break;
                }
            }
        }

        log.info("Beginning common startup of twin");
        // Set up directory for file logs (dumps of contents of incoming or
        // outgoing messages).
        if (logDirectory != null) {
            if (!Files.exists(logDirectory)) {
                try {
                    Files.createDirectories(logDirectory);
                } catch (final IOException e) {
                    log.warn("Could not create log directory {}. Continuing without file logging.");
                    logDirectory = null;
                }
            } else if (!Files.isDirectory(logDirectory) || !Files.isWritable(logDirectory)) {
                log.warn("Trying to use a file as log directory, or directory not writable: {}. Continuing without file logging.", logDirectory);
                logDirectory = null;
            } else {
                log.info("Logging all messages to directory {}", logDirectory);
            }
        }
    }

    /**
     * The main method of the main class.  Shows the help text and returns
     * with an error, so that scripts fail and someone checks what's up.
     */
    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 1;
    }

    /**
     * External entry point for the main class.  Parses command-line
     * parameters and invokes the `call` method.
     *
     * @param args the command-line parameters as passed by the user
     */
    public static void main(final String[] args) {
        final Main app = new Main();
        final int exitCode = new CommandLine(app)
            .setExecutionStrategy(app::executionStrategy) // perform common initialization
            .execute(args);
        System.exit(exitCode);
    }

    /**
     * Log a file into the given log directory.  Does nothing if {@link
     * Main#logDirectory} is not set.
     *
     * @param name A string that can be used as part of a filename, does not
     *  need to be unique.  Should not contain characters that are illegal in
     *  file names, e.g., avoid colons (:) or slashes (/).
     * @param contents The content of the file to be written.  Will be
     *  converted to a String via `toString`.
     */
    public static void logFile(final String name, final Object contents) {
        if (Main.logDirectory == null) return;
        final String prefix = LocalDateTime.now().toString()
            .replace(":", "-"); // make Windows less unhappy
        final Path path = logDirectory.resolve(prefix + "--" + name);
        try (FileWriter out = new FileWriter(path.toFile())) {
            out.write(contents.toString());
            log.trace("Wrote log file {}", path);
        } catch (final IOException e) {
            log.warn("Error while trying to create data file in log directory", e);
        }
    }
}
