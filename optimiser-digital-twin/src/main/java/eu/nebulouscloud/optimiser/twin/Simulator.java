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
        // 1. Read traces file
        // 2. Create traces.db
        // 3. Read solver message file
        // 4. Create config.db
        // 5. Run simulation
        System.out.println("Running simulation ...");
        return 0;
    }

}
