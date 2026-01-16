twinjar := "optimiser-digital-twin/dist/optimiser-digital-twin-all.jar"

# Build the jar
build:
    ./gradlew build

# Run the twin, output trace on screen
run:
    java -jar {{twinjar}} simulate trace.db scenario.db calibration.db

# Analyze tracefile
analyze-trace tracefile:
    java -jar {{twinjar}} analyze-traces {{tracefile}}
    # optimiser-digital-twin/src/test/resources/logs.jsonl

# Create scenario.db from csv scenario file
import-scenario scenariofile:
    java -jar {{twinjar}} import-deployment scenario.db --deployment-file {{scenariofile}}
    # optimiser-digital-twin/src/test/resources/deployment-example.csv

# Create trace.db from jsonl trace file
import-trace tracefile:
    java -jar {{twinjar}} import-traces trace.db {{tracefile}}
    # optimiser-digital-twin/src/test/resources/logs.jsonl

# Create calibratin.db from csv calibration file
import-calibration calibrationfile:
    java -jar {{twinjar}} import-calibration calibration.db {{calibrationfile}}
    # optimiser-digital-twin/src/test/resources/calibration.csv
