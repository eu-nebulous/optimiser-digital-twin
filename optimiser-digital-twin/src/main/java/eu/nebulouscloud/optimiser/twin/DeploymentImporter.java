package eu.nebulouscloud.optimiser.twin;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import groovyjarjarpicocli.CommandLine.ParentCommand;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Slf4j
@Command(name = "import-deployment",
    description = "Create deployment database from solver solution",
    mixinStandardHelpOptions = true)
public class DeploymentImporter implements Callable<Integer> {
    @ParentCommand
    private App app;

    @Parameters(description = "A file containing a solver message with machine configurations")
    private Path configurationFile;

    @Override
    public Integer call() {
        System.out.println("Import deployment configuration " + configurationFile + " ...");
        return 0;
    }
}
