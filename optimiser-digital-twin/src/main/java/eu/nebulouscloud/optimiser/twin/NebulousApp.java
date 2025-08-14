package eu.nebulouscloud.optimiser.twin;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * This class serves as a container for app-specific information.  Solver
 * messages, which contain the calculated node requirements, refer to
 * attributes of KubeVela components that are defined in the initial
 * application definition.  Hence, we need to keep information about the
 * NebulOuS application around to reconfigure the digital twin in response to
 * solver messages.
 */
@Slf4j
public class NebulousApp {

    // ----------------------------------------
    // App message parsing stuff

    /** Location of the kubevela yaml file in the app creation message (String) */
    private static final JsonPointer kubevela_path = JsonPointer.compile("/content");
    /** Location of the variables (optimizable locations) of the kubevela file
     * in the app creation message. (Array of objects) */
    private static final JsonPointer variables_path = JsonPointer.compile("/variables");
    /** Locations of the UUID and name in the app creation message (String) */
    private static final JsonPointer uuid_path = JsonPointer.compile("/uuid");
    private static final JsonPointer name_path = JsonPointer.compile("/title");
    /** The YAML converter */
    // Note that instantiating this is apparently expensive, so we do it only once
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    private static final Map<String, NebulousApp> apps = new HashMap<>();

    /** General-purpose object mapper */
    @Getter private static final ObjectMapper jsonMapper = new ObjectMapper();

    /** The uuid of the app (generated; assumed unique) */
    @Getter private final String uuid;
    /** The name of the app (human-readable, non-unique) */
    @Getter private final String name;
    /** The app creation message */
    @Getter private final JsonNode appMessage;
    /** The kubevela definition */
    @Getter private final JsonNode kubevela;
    /** The array of KubeVela variables in the app message. */
    @Getter private Map<String, JsonNode> kubevelaVariables = new HashMap<>();


    private NebulousApp(String uuid, JsonNode appMessage, JsonNode kubevela, ArrayNode parameters) {
        this.uuid = uuid;
        this.name = appMessage.at(name_path).textValue();
        this.appMessage = appMessage;
        this.kubevela = kubevela;
        for (JsonNode p : parameters) {
            kubevelaVariables.put(p.get("key").asText(), p);
        }
    }

    /**
     * Create a NebulousApp object that holds the information about the
     * application defined in appMessage.
     *
     * @throws JsonProcessingException
     * @throws JsonMappingException
     */
    public static NebulousApp fromAppMessage(JsonNode appMessage) throws JsonMappingException, JsonProcessingException {
        String kubevelaString = appMessage.at(kubevela_path).textValue();
        String uuid = appMessage.at(uuid_path).textValue();
        ArrayNode parameters = appMessage.withArray(variables_path);
        if (kubevelaString == null) {
            log.error("Could not find kubevela or parameters in app creation message; aborting");
            return null;
        }
        JsonNode kubevela = yamlMapper.readTree(kubevelaString);
        synchronized(NebulousApp.class) {
            NebulousApp result = new NebulousApp(uuid, appMessage, kubevela, parameters);
            apps.put(uuid, result);
            return result;
        }
    }

    /**
     * Find a previously-instantiated NebulousApp via its uuid.
     */
    public static synchronized NebulousApp fromUuid(String uuid) {
        return apps.get(uuid);
    }

    /**
     * Return a kubevela solution, with the variable values of the original
     * kubevela app deployment scenario replaced by values supplied by the
     * solver.
     *
     * <p>Note that this code is copied from
     * https://github.com/eu-nebulous/optimiser-controller/blob/main/optimiser-controller/src/main/java/eu/nebulouscloud/optimiser/controller/NebulousApp.java.
     */
    public ObjectNode rewriteKubevelaWithSolution(ObjectNode variableValues) {
        ObjectNode freshKubevela = kubevela.deepCopy();
        for (Map.Entry<String, JsonNode> entry : variableValues.properties()) {
            String key = entry.getKey();
            JsonNode replacementValue = entry.getValue();
            JsonNode param = kubevelaVariables.get(key);
            // The solver sends all defined AMPL variables, not only the ones
            // that correspond to KubeVela locations
            if (param == null) continue;
            String pathstr = param.at("/path").asText();
            // The "application_deployment_price" variable (with meaning:
            // "price"), does not have a KubeVela path associated.
            if (pathstr == null || pathstr.isEmpty()) continue;
            JsonPointer path = JsonPointer.compile(pathstr);
            JsonNode nodeToBeReplaced = freshKubevela.at(path);
            boolean doReplacement = true;

            if (nodeToBeReplaced == null) {
                // Didn't find location in KubeVela file (should never happen)
                log.warn("Location {} not found in KubeVela, cannot replace with value {}",
                    key, replacementValue);
                doReplacement = false;
            } else {
                String meaning = param.at("/meaning").asText();
                if (KubevelaAnalyzer.isKubevelaInteger(meaning) && replacementValue.isFloatingPointNumber()) {
                    // Workaround for
                    // https://github.com/eu-nebulous/optimiser-controller/issues/119
                    // -- we didn't get the dsl file so we can't check if we
                    // (in the AMPL file) or the solver in its return value
                    // produce the stray float value.
                    replacementValue = new LongNode(replacementValue.asLong());
                }
                if (meaning.equals("memory")) {
                    // Special case: the solver delivers a number for memory, but
                    // KubeVela wants a number with a unit, so we have to add one.
                    // Also, we have cases where the initial node size is
                    // specified in GB but the formulas and boundaries are
                    // expressed in MB (and vice versa), so we have to use a
                    // heuristic for guessing which unit the user meant.
                    if (!(replacementValue.asText().endsWith("Mi")
                          || replacementValue.asText().endsWith("Gi"))) {
                        if (replacementValue.asDouble() <= 512) {
                            replacementValue = new TextNode(replacementValue.asText() + "Gi");
                        } else {
                            replacementValue = new TextNode(replacementValue.asText() + "Mi");
                        }
                    }
                }
            }              // Handle other special cases here, as they come up
            if (doReplacement) {
                ObjectNode parent = (ObjectNode)freshKubevela.at(path.head());
                String property = path.last().getMatchingProperty();
                parent.replace(property, replacementValue);
            }
        }
        return freshKubevela;
    }
}
