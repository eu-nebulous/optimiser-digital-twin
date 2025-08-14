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
import eu.nebulouscloud.exn.handlers.ConnectorHandler;
import eu.nebulouscloud.exn.settings.StaticExnConfig;
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

    /** A counter to create unique names for SyncedPublisher instances. */
    private final AtomicInteger publisherNameCounter = new AtomicInteger(1);

    /** if non-null, signals after the connector is stopped */
    private CountDownLatch synchronizer = null;

    private static final ObjectMapper mapper = new ObjectMapper();

    /** The topic where we listen for app creation messages. */
    // Note that there is another, earlier app creation message sent via the
    // channel `eu.nebulouscloud.ui.application.new`, but its format is not
    // yet defined as of 2024-01-08.
    public static final String app_creation_channel = "eu.nebulouscloud.ui.dsl.generic";
    /** The topic with an application's relevant performance indicators. */
    public static final String performance_indicators_channel =
        "eu.nebulouscloud.optimiser.utilityevaluator.performanceindicators";
    /** The topic with incoming solver solution messages.  See
      * https://openproject.nebulouscloud.eu/projects/nebulous-collaboration-hub/wiki/2-solvers */
    public static final String solver_solution_channel = "eu.nebulouscloud.optimiser.solver.solution";
    /** The per-app status channel, read by at least the UI and the solver. */
    public static final String app_status_channel = "eu.nebulouscloud.optimiser.controller.app_state";

    /**
     * Create a connection to ActiveMQ via the exn middleware, and set up the
     * initial publishers and consumers.
     *
     * @param host the host of the ActiveMQ server (probably "localhost")
     * @param port the port of the ActiveMQ server (usually 5672)
     * @param name the login name to use
     * @param password the login password to use
     */
    public ExnConnector(final String host, final int port, final String name, final String password) {
        conn = new Connector(
            "optimiser_controller",
            new ConnectorHandler() {
                public void onReady(final Context context) {
                    ExnConnector.this.context_ = context;
                    synchronized(ExnConnector.this) {
                        ExnConnector.this.notifyAll();
                    }
                    log.info("Optimiser-controller connected to ActiveMQ, got connection context {}", context);
                }
            },
            List.of(
                // Asynchronous topics for sending out controller status.
                // Synchronous publishers are created dynamically, not added
                // here.
            ),
            List.of(
                new Consumer("ui_app_messages", app_creation_channel,
                    new AppCreationMessageHandler(), true, true),
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
     * A message handler that processes app creation messages coming in via
     * `eu.nebulouscloud.ui.dsl.generic`.  Such messages contain, among
     * others, the KubeVela YAML definition and mapping from KubeVela
     * locations to AMPL variables.
     *
     * <p>Upon receiving a message, extract the list of components and the
     * static part of the deployment scenario, then wait for the synthetic
     * solver solution message sent by the optimiser-controller for the
     * dynamic parts of the deployment scenario.
     */
    public class AppCreationMessageHandler extends Handler {
        @Override
        public void onMessage(final String key, final String address, final Map body, final Message message, final Context context) {
            try {
                log.info("App creation message received");
                final JsonNode appMessage = mapper.valueToTree(body);
                final String appID = appMessage.at("/uuid").asText();
                MDC.put("appId", appID);
                Main.logFile("app-message-" + appID + ".json", appMessage.toPrettyString());
                // TODO: initialize twin with list of components.  Don't
                // calculate the initial deployment, we'll wait for the
                // synthetic solver solution message from the
                // optimiser-controller
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
            // We'll talk a lot with SAL etc, so we should maybe fire up a
            // thread so as not to block here.
            try {
                final ObjectNode json_body = mapper.convertValue(body, ObjectNode.class);
                final String app_id = message.property("application").toString(); // should be string already, but don't want to cast
                if (app_id == null) {
                    log.warn("Received solver solution without 'application' message property, discarding it");
                    return;
                }
                MDC.put("appId", app_id);
                Main.logFile("solver-solution-" + app_id + ".json", json_body.toPrettyString());
                // Three cases here:
                // - The initial synthetic solution: initialize twin, start
                //   calibrating
                // - A real solution (deploy = true): reinitialize twin,
                //   discard calibrated parameters, wait for deployment to
                //   finish
                // - A what-if solution (deploy = false): evaluate it
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
