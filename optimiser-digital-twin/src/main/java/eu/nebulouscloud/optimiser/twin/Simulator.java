package eu.nebulouscloud.optimiser.twin;

import java.util.concurrent.Callable;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Slf4j
@Command(name = "simulate",
    description = "Execute one simulation run",
    mixinStandardHelpOptions = true)
public class Simulator implements Callable<Integer> {

    @ParentCommand
    private Main app;

    @Override
    public Integer call() throws Exception {
        System.out.println("Running simulation ...");
        return 0;
    }

}
