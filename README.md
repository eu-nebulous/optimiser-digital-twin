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
