package eu.nebulouscloud.optimiser.twin;

import java.nio.file.Path;
import java.util.concurrent.Callable;

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
    private App app;

    @Parameters(description = "A file containing trace logs")
    private Path traceFile;

    @Override
    public Integer call() throws Exception {
        System.out.println("Import trace file " + traceFile + " ...");
        return 0;
    }

}
