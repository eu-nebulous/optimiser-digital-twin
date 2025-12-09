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

List all sub-commands and common options:

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar -h
```

## Creating a deployment scenario database

The deployment scenario lists the application's components and, for each
component, the number of replicas and the characteristics of the machine type
where that component is deployed.  During calibration, the deployment scenario
mirrors the real application's deployment; during evaluation, the solver
supplies alternative values for deployment parameters that are specified to be
variable.

A scenario database for the digital twin can be created from an application
creation message and solver solution by running the following command:

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar import-deployment scenario.db optimiser-digital-twin/src/test/resources/app-creation-message.json --solution optimiser-digital-twin/src/test/resources/sample-solution.json
```

Note that the `--solution` parameter is optional; if no solver solution is
supplied, the deployment scenario is created from the app creation message
alone.  (This mirrors the initial deployment of the application.)

## Creating a trace database

Traces are recorded in [jsonl](https://jsonlines.org) format.  They are
converted into sqlite format via the following command:

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar import-traces logs.db optimiser-digital-twin/src/test/resources/logs.jsonl
```

## Creating a calibration database

The calibration database provides the constant and variable cost factors for
replaying an event on the digital twin.  It can be created from a CSV file
with one entry for each component.

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar import-calibration calibration.db optimiser-digital-twin/src/test/resources/calibration.csv
```

## Running a simulation

With a deployment scenario, traces, and calibration values, we can run a
simulation.  The simulator will print to stdout log entries in the same jsonl
format as the recorded traces.

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar simulate trace.db scenario.db calibration.db
```

## Trace analysis

A trace of events, in jsonl format, can be analyzed for total runtime,
duration of individual events per component, etc.  Note that both recorded
traces and the output of the simulator can be analyzed in this way.

```sh
java -jar optimiser-digital-twin/dist/optimiser-digital-twin-all.jar analyze-traces optimiser-digital-twin/src/test/resources/logs.jsonl
```
