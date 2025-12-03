package eu.nebulouscloud.optimiser.twin;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Slf4j
@Command(name = "serve",
    aliases = {"s"},
    description = "Start microservice in the NebulOuS platform",
    mixinStandardHelpOptions = true)
public class Server implements Callable<Integer> {

    /** Reference to main app set up by PicoCLI.  This lets us ask for the
      * ActiveMQ connector. */
    @ParentCommand
    private Main app;

    @Option(names = {"--activemq-host"},
            description = "The hostname of the ActiveMQ server.  Can also be set via the @|bold ACTIVEMQ_HOST|@ environment variable.",
            paramLabel = "ACTIVEMQ_HOST",
            defaultValue = "${ACTIVEMQ_HOST:-localhost}")
    private String activemq_host;

    @Option(names = {"--activemq-port"},
            description = "The port of the ActiveMQ server.  Can also be set via the @|bold ACTIVEMQ_PORT|@ environment variable.",
            paramLabel = "ACTIVEMQ_PORT",
            defaultValue = "${ACTIVEMQ_PORT:-5672}")
    private int activemq_port;

    @Option(names = {"--activemq-user"},
            description = "The user name for the ActiveMQ server. Can also be set via the @|bold ACTIVEMQ_USER|@ environment variable.",
            paramLabel = "ACTIVEMQ_USER",
            defaultValue = "${ACTIVEMQ_USER}")
    private String activemq_user;

    @Option(names = {"--activemq-password"},
            description = "The password for the ActiveMQ server. Can also be set via the @|bold ACTIVEMQ_PASSWORD|@ environment variable.",
            paramLabel = "ACTIVEMQ_PASSWORD",
            defaultValue = "${ACTIVEMQ_PASSWORD}")
    private String activemq_password;

    @Option(names = {"--trace-dir"},
        description = "Directory where the digital twin picks up trace files.  Can also be set via the @|bold TRACEDIR|@ variable.",
        paramLabel = "TRACEDIR",
        defaultValue = "${TRACEDIR}")
    private static Path incomingTraceDirectory;

    /** Synchronization between directory monitor and trace importer
      * threads */
    private final BlockingQueue<Path> fileQueue = new LinkedBlockingQueue<>();

    /** Flag for orderly shutdown of internal threads */
    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * The ActiveMQ connector.
     *
     * @return the ActiveMQ connector wrapper, or null if running offline.
     */
    @Getter
    private static ExnConnector activeMQConnector = null;

    @Override
    public Integer call() {
        // Start connection to ActiveMQ if possible.
        if (activemq_user != null && activemq_password != null) {
            log.info("Preparing ActiveMQ connection: host={} port={}",
                activemq_host, activemq_port);
            activeMQConnector
              = new ExnConnector(activemq_host, activemq_port,
                  activemq_user, activemq_password);
        } else {
            log.debug("ActiveMQ login info not set, only operating locally.");
        }
        CountDownLatch exn_synchronizer = new CountDownLatch(1);
        if (activeMQConnector != null) {
            log.debug("Starting connection to ActiveMQ");
            activeMQConnector.start(exn_synchronizer);
        } else {
            log.error("ActiveMQ connector not initialized (no connection parameters passed?) so we're unresponsive. Will keep running to keep CI/CD happy but don't expect anything more from me.");
        }
        if (incomingTraceDirectory == null) {
            log.error("Trace directory not set, not watching for traces so the digital twin will not calibrate.");
        } else {
            log.info("Watching for traces in {}", incomingTraceDirectory);
            Thread.ofPlatform().daemon().start(this::watchForFiles);
            Thread.ofPlatform().daemon().start(this::processFiles);
        }
        // Note that we try to synchronize, even if we didn't connect to
        // ActiveMQ.  This is so that the container can be deployed.  (If the
        // container terminates, the build registers as unsuccessful.)
        log.info("Optimiser-digital-twin initialization complete, waiting for messages");
        try {
            exn_synchronizer.await();
        } catch (InterruptedException e) {
            // ignore
        }
        running.set(false);
        return 0;
    }

    private void watchForFiles() {
        if (incomingTraceDirectory == null) return;
        log.info("Starting file watcher process");
        try (var watchService = FileSystems.getDefault().newWatchService()) {
            incomingTraceDirectory.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            while (running.get()) {
                WatchKey key = watchService.poll(1, TimeUnit.SECONDS);
                if (key == null) continue;

                for (var event : key.pollEvents()) {
                    var kind = event.kind();
                    if (kind == StandardWatchEventKinds.OVERFLOW) continue; // TODO: manually read directory contents?
                    if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                        Path fullPath = incomingTraceDirectory.resolve((Path)event.context());
                        if (fullPath.toString().endsWith(".jsonl")) {
                            log.info("Received new trace file {}", fullPath);
                            fileQueue.offer(fullPath);
                        }
                    }
                }
                key.reset();
            }
        } catch (IOException | InterruptedException e) {
            log.error("Unexpected error in file watcher process; terminating", e);
	}
    }

    private void processFiles() {
        log.info("Starting trace importer process");
        while (running.get()) {
            try {
                Path file = fileQueue.poll(1, TimeUnit.SECONDS);
                if (file == null) continue;
                long nTraces = TraceImporter.storeLog(file);
                log.info("Import {} traces from files {}", nTraces, file);
                Files.delete(file);
            } catch (InterruptedException e) {
                // ignore; we terminate when running = false
            } catch (IOException e) {
                log.warn("Error while reading trace file", e);
	    }
        }
    }

}
