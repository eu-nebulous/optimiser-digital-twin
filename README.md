A digital twin in the making.

# Building

To compile, install a JDK (Java Development Kit) version 21 or greater on the
build machine.  Compiling needs internet access since gradle will download the
necessary dependencies at build time.

```sh
# Compile:
./gradlew assemble
# Compile and test:
./gradlew build
```

The build step produces a self-contained jar file
`optimiser-digital-twin/dist/optimiser-digital-twin-all.jar` that can be
started with `java -jar`.

# Building the container

A container can be built and run with the following commands:

```sh
docker build -t optimiser-digital-twin -f Dockerfile .
docker run --rm optimiser-digital-twin
```

Starting the container will start the digital twin in server mode.  The
following environment variables are used to connect to the ActiveMQ server of
the Nebulous system: `ACTIVEMQ_HOST`, `ACTIVEMQ_PORT`, `ACTIVEMQ_USER`,
`ACTIVEMQ_PASSWORD`

# Running from the command line

All twin simulator functions are accessible by running the jar file produced
by the build.

List all sub-commands and common options:

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar -h
```

The project root also contains a
[Justfile](https://just.systems/man/en/introduction.html) that wraps the most
common commands; see the output of `just --list`.

## Creating a deployment scenario database

The deployment scenario lists the application's components and, for each
component, the number of replicas and the characteristics of the machine type
where that component is deployed.  During calibration, the deployment scenario
mirrors the real application's deployment; during evaluation, the solver
supplies alternative values for deployment parameters that are specified to be
variable.

There are two ways to specify a deployment scenario: either via the
application creation message (and, optionally, the solver solution) sent by
NebulOuS, or via a CSV file containing the machine characteristics for each
component.

### Importing a CSV file

A deployment scenario is specified in a CSV file with the following header:

```csv
Component,Replicas,Cores,Memory
```

`Component` is a string, the other fields of each line are integers.

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar import-deployment scenario.db --deployment-file optimiser-digital-twin/src/test/resources/deployment-example.csv
```

Via just:
```sh
just import-scenario optimiser-digital-twin/src/test/resources/deployment-example.csv
```

### Importing NebulOuS scenarios

A scenario database for the digital twin can be created from a NebulOuS
application creation message and solver solution by running the following
command:

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar import-deployment scenario.db --app-creation-message optimiser-digital-twin/src/test/resources/app-creation-message.json --solution optimiser-digital-twin/src/test/resources/sample-solution.json
```

Note that the `--solution` parameter is optional; if no solver solution is
supplied, the deployment scenario is created from the app creation message
alone.  (This mirrors the initial deployment of the application.)

## Creating a trace database

Traces are recorded in [jsonl](https://jsonlines.org) format.  They are
converted into sqlite format via the following command:

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar import-traces trace.db optimiser-digital-twin/src/test/resources/logs.jsonl
```

Via just:
```sh
just import-trace optimiser-digital-twin/src/test/resources/logs.jsonl 
```

## Creating a calibration database

The calibration database provides the constant and variable cost factors for
replaying an event on the digital twin.  It can be created from a CSV file
with one entry for each component, with a header like this:

```csv
component,constant_factor,variable_factor
```

`component` is a string, with the same name as a component in the scenario database.  The other two fields are floating-point numbers.

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar import-calibration calibration.db optimiser-digital-twin/src/test/resources/calibration.csv
```

Via just:
```sh
just import-calibration optimiser-digital-twin/src/test/resources/calibration.csv
```

## Running a simulation

With a deployment scenario database, a trace database and a calibration
database, we can run a simulation.  The simulator will print to stdout log
entries in the same jsonl format as the recorded traces.

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar simulate trace.db scenario.db calibration.db
```

Via just:
```sh
just run output.jsonl
```

## Trace analysis

A trace of events, in jsonl format, can be analyzed for total runtime,
duration of individual events per component, etc.  Note that both the output
of the simulator and traces recorded from the application can be analyzed in
this way.

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar analyze-traces optimiser-digital-twin/src/test/resources/logs.jsonl
```

Via just:
```sh
just analyze-trace optimiser-digital-twin/src/test/resources/logs.jsonl
```
