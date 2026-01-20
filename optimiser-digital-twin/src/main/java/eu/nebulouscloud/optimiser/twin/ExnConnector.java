package eu.nebulouscloud.optimiser.twin;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.MDC;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import eu.nebulouscloud.exn.Connector;
import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import eu.nebulouscloud.exn.core.Publisher;
import eu.nebulouscloud.exn.handlers.ConnectorHandler;
import eu.nebulouscloud.exn.settings.StaticExnConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A class that connects to the EXN middleware and starts listening to
 * messages from the ActiveMQ server.
 *
 * <p>This class drives the calibration and simulation behavior of the twin:
 * the `Consumer` objects created in {@link ExnConnector#ExnConnector} receive
 * incoming messages and react to them, sending out messages in turn.
 */
@Slf4j
public class ExnConnector {

    /** The Connector used to talk with ActiveMQ */
    private final Connector conn;

    private Context context_ = null;
    /**
     * Safely obtain the connection Context object.  Since the {@link
     * #context_} field is set asynchronously after the ExnConnector
     * constructor has finished, there is a race condition where we might hit
     * a null value if using the field directly.
     */
    public Context getContext() {
        if (context_ == null) {
            synchronized(this) {
                while (context_ == null) {
                    try {
                        wait();
                    } catch (final InterruptedException e) {
                        log.error("Caught InterruptException while waiting for ActiveMQ connection Context; looping", e);
                    }
                }
            }
        }
        return context_;
    }

    /** if non-null, signals after the connector is stopped */
    private CountDownLatch synchronizer = null;

    private static final ObjectMapper mapper = new ObjectMapper();

    /** The topic with an application's relevant performance indicators. */
    public static final String performance_indicators_channel =
        "eu.nebulouscloud.optimiser.utilityevaluator.performanceindicators";
    /** The topic with incoming solver solution messages.  See
      * https://openproject.nebulouscloud.eu/projects/nebulous-collaboration-hub/wiki/2-solvers */
    public static final String solver_solution_channel = "eu.nebulouscloud.optimiser.solver.solution";
    /** The per-app status channel, read by at least the UI and the solver. */
    public static final String app_status_channel = "eu.nebulouscloud.optimiser.controller.app_state";
    /** The topic for receiving the app creation message after notifying the
     * optimiser-controller we started */
    public static final String twin_init_channel = "eu.nebulouscloud.optimiser.controller.twin_init";


    /** The status channel for notifying the optimiser-controller that the
     * digital twin started.  We send out an app's creation message on the
     * channel named by {@link #ampl_message_channel} when getting the
     * "started" message from a digital twin. */
    public static final String twin_status_channel = "eu.nebulouscloud.optimiser.twin.state";

    /**
      * The Message producer for notifying the optimiser-controller that the digital twin started
      */
    @Getter
    private final Publisher statusPublisher = new Publisher("controller_ampl", twin_status_channel, true, true);

    private final ObjectMapper jsonMapper = new ObjectMapper();

    /**
     * Create a connection to ActiveMQ via the exn middleware, and set up the
     * initial publishers and consumers.
     *
     * @param host the host of the ActiveMQ server (probably "localhost")
     * @param port the port of the ActiveMQ server (usually 5672)
     * @param name the login name to use
     * @param password the login password to use
     * @param appId the application id of the application we're twinning
     */
    public ExnConnector(final String host, final int port, final String name, final String password, final String appId) {
        MDC.put("appId", appId);
        conn = new Connector(
            "optimiser_digital_twin",
            new ConnectorHandler() {
                public void onReady(final Context context) {
                    ExnConnector.this.context_ = context;
                    synchronized(ExnConnector.this) {
                        ExnConnector.this.notifyAll();
                    }
                    // Informing optimiser-controller that we're ready
                    ObjectNode msg = jsonMapper.createObjectNode();
                    msg.put("state", "started");
                    getStatusPublisher().send(jsonMapper.convertValue(msg, Map.class), appId, true);
                    log.info("Optimiser digital twin connected to ActiveMQ, got connection context {}", context);
                }
            },
            List.of(
                // Asynchronous topics for sending out messages.  Synchronous
                // publishers are created dynamically, not added here.
                statusPublisher),
            List.of(
                new Consumer("ui_app_messages", twin_init_channel,
                    new InitMessageHandler(), true, true),
                new Consumer("performance_indicator_messages", performance_indicators_channel,
                    new PerformanceIndicatorMessageHandler(), true, true),
                new Consumer("solver_solution_messages", solver_solution_channel,
                    new SolverSolutionMessageHandler(), true, true),
                new Consumer("app_status_messages", app_status_channel,
                    new AppStatusMessageHandler(), true, true)),
            true,
            true,
            new StaticExnConfig(host, port, name, password, 15, "eu.nebulouscloud"));
    }

    /**
     * Connect to ActiveMQ and activate all publishers and consumers.  It is
     * an error to start the controller more than once.
     *
     * @param synchronizer if non-null, a countdown latch that will be
     *  signaled when the connector is stopped by calling {@link
     *  CountDownLatch#countDown} once.
     */
    public synchronized void start(final CountDownLatch synchronizer) {
        this.synchronizer = synchronizer;
        conn.start();
        log.debug("ExnConnector started.");
    }

    /**
     * Disconnect from ActiveMQ and stop all Consumer processes.  Also count
     * down the countdown latch passed in the {@link
     * #start(CountDownLatch)} method if applicable.
     */
    public synchronized void stop() {
        conn.stop();
        if (synchronizer != null) {
            synchronizer.countDown();
        }
        log.debug("ExnConnector stopped.");
    }

    // ----------------------------------------
    // Message Handlers

    /**
     * A message handler that processes init messages.
     *
     * <p> An init message is sent by the optimiser-controller on {@code
     * eu.nebulouscloud.optimiser.controller.twin_init} after receiving a
     * "state: started" message on {@code eu.nebulouscloud.twin.state}.  The
     * message has a {@code dsl} key with the app creation message, and a
     * {@code solution} key with the synthetic solver message with the initial
     * deployment.
     *
     * <p>Upon receiving a message, extract the list of components and the
     * static part of the deployment scenario from the app creation message,
     * and the dynamic parts of the deployment scenario from the synthetic
     * solver solution.
     */
    public class InitMessageHandler extends Handler {
        @Override
        public void onMessage(final String key, final String address, final Map body, final Message message, final Context context) {
            try {
                log.debug("Initialization message received");
                final JsonNode initMessage = mapper.valueToTree(body);
                final JsonNode dslMessage = initMessage.at("/dsl");
                final JsonNode solverMessage = initMessage.at("/solution");
                final String appID = dslMessage.at("/uuid").asText();
                // TODO: check if appID equals our own
                MDC.put("appId", appID);
                log.info("Twin received initialization message for app id {}", appID);
                Main.logFile("init-message-" + appID + ".json", initMessage.toPrettyString());
                // TODO: initialize twin with list of components, and
                // calculate the initial deployment.  Initialize twin and wait
                // for log files to start calibrating.
            } catch (final RuntimeException e) {
                log.error("Error while receiving app creation message", e);
            } finally {
                MDC.clear();
            }
        }
    }

    /**
     * A handler that receives the performance indicators that the utility
     * evaluator sends.  If the application object already exists (usually the
     * case), start initial deployment, otherwise store the performance
     * indicators so the initial app creation message can pick them up.
     */
    public class PerformanceIndicatorMessageHandler extends Handler {
        @Override
        public void onMessage(final String key, final String address, final Map body, final Message message, final Context context) {
            try {
                Object appIdObject = null;
                try {
                    appIdObject = message.property("application");
                    if (appIdObject == null) appIdObject = message.subject();
                } catch (final ClientException e) {
                    log.error("Received performance indicator message without application property, aborting");
                    return;
                }
                String appId = null;
                if (appIdObject == null) {
                    log.error("Received performance indicator message without application property, aborting");
                    return;
                } else {
                    appId = appIdObject.toString(); // should be a string already
                }
                MDC.put("appId", appId);
                final JsonNode appMessage = mapper.valueToTree(body);
                Main.logFile("performance-indicators-" + appIdObject + ".json", appMessage.toPrettyString());
                // TODO: do we need to process this message?
            } catch (final Exception e) {
                log.error("Error while processing solver solutions message", e);
            } finally {
                MDC.clear();
            }
        }
    }

    /**
     * A message handler for incoming messages from the solver, containing
     * mappings from variable names to new values.  This solution is then
     * evaluated.
     */
    public class SolverSolutionMessageHandler extends Handler {
        @Override
        public void onMessage(final String key, final String address, final Map body, final Message message, final Context context) {
            try {
                final ObjectNode json_body = mapper.convertValue(body, ObjectNode.class);
                final String app_id = message.property("application").toString(); // should be string already, but don't want to cast
                if (app_id == null) {
                    log.warn("Received solver solution without 'application' message property, discarding it");
                    return;
                }
                MDC.put("appId", app_id);
                Main.logFile("solver-solution-" + app_id + ".json", json_body.toPrettyString());
                // Two cases here:
                // - A real solution (deploy = true): reinitialize twin,
                //   discard calibrated parameters, wait for deployment to
                //   finish
                // - A what-if solution (deploy = false): simulate it
            } catch (final Exception e) {
                log.error("Error while processing solver solutions message", e);
            } finally {
                MDC.clear();
            }
        }
    }

    public class AppStatusMessageHandler extends Handler {
        @Override
        public void onMessage(final String key, final String address, final Map body, final Message message, final Context context) {
            // We'll talk a lot with SAL etc, so we should maybe fire up a
            // thread so as not to block here.
            try {
                final ObjectNode json_body = mapper.convertValue(body, ObjectNode.class);
                final String app_id = message.property("application").toString(); // should be string already, but don't want to cast
                if (app_id == null) {
                    log.warn("Received app state without 'application' message property, discarding it");
                    return;
                }
                MDC.put("appId", app_id);
                // Suspend/resume twin: we don't simulate while redeploying, etc.
            } catch (final Exception e) {
                log.error("Error while processing solver solutions message", e);
            } finally {
                MDC.clear();
            }
        }
    }

}
