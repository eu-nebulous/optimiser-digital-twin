/**
 * This package implements the NebulOuS digital twin.
 *
 * <p>The digital twin contains a data-driven ABS model that replays
 * application traces against a configurable <em>deployment scenario</em>.  A
 * deployment scenario is an assignment of the application's components to VMs
 * with specific attributes (memory, cores).  The twin runs in two modes:
 *
 * <ul>
 * <li>Calibration mode: running with a deployment scenario modeling the
 * current deployment, calibrate the model parameters so that the twin
 * conforms to the real system.
 *
 * <li>Evaluation mode: running with the calibrated parameters against an
 * alternative deployment scenario to reach a verdict on the suggested
 * deployment scenario.
 * </ul>
 *
 * <p>The main entry point is the class {@link App}.  The class {@link
 * ExnConnector} implements communication with the rest of the Nebulous system
 * by setting up the necessary publishers and message handlers to send and
 * receive ActiveMQ messages.
 *
 * @author Rudolf Schlatte
 */
package eu.nebulouscloud.optimiser.twin;
