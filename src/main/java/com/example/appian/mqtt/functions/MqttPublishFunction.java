package com.example.appian.mqtt.functions;

import com.appiancorp.suiteapi.expression.annotations.Category;
import com.appiancorp.suiteapi.expression.annotations.Function;
import com.appiancorp.suiteapi.expression.annotations.Parameter;
import com.example.appian.mqtt.core.CentralConnectionManager;
import com.example.appian.mqtt.core.SocketHolder;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MQTT Publish Expression Function for SAIL.
 * <p>
 * Available in the Appian Expression Editor as:
 * <pre>
 *   mqttPublish("tcp://broker:1883", "clientId", "topic/name", "{\"temp\":25}", 1)
 * </pre>
 * <p>
 * Returns a JSON string with the publish result:
 * <ul>
 *   <li>Success: {"status":"SUCCESS","topic":"...","messageId":"..."}</li>
 *   <li>Error: {"status":"ERROR","errorMessage":"..."}</li>
 * </ul>
 * <p>
 * Rules for Custom Functions:
 * <ul>
 *   <li>Must NOT throw exceptions — returns error JSON instead</li>
 *   <li>Note: this function has side effects (publishes to MQTT). Use in
 *       expression rules or interface save expressions, not in repeated evaluations.</li>
 * </ul>
 */
@Category("mqttFunctions")
public class MqttPublishFunction {

    private static final Logger LOG = Logger.getLogger(MqttPublishFunction.class.getName());
    private static final int DEFAULT_CONNECTION_TIMEOUT = 10;
    private static final long PUBLISH_TIMEOUT_MS = 10_000L;

    /**
     * Publishes a message to an MQTT topic.
     *
     * @param brokerUrl MQTT broker URL (e.g. tcp://broker.example.com:1883)
     * @param clientId  Unique MQTT client identifier
     * @param topic     MQTT topic to publish to
     * @param payload   Message payload (JSON or plain text)
     * @param qos       Quality of Service level (0, 1, or 2)
     * @return JSON string with publish result
     */
    @Function
    public String mqttPublish(
            @Parameter(name = "brokerUrl") String brokerUrl,
            @Parameter(name = "clientId") String clientId,
            @Parameter(name = "topic") String topic,
            @Parameter(name = "payload") String payload,
            @Parameter(name = "qos") Long qos) {

        int qosLevel = (qos != null) ? qos.intValue() : 0;

        // Validate QoS range
        if (qosLevel < 0 || qosLevel > 2) {
            return buildErrorJson("Invalid QoS level: " + qosLevel + ". Must be 0, 1, or 2.");
        }

        try {
            SocketHolder socketHolder = CentralConnectionManager.getInstance().getOrConnect(
                    brokerUrl, clientId, null, null,
                    DEFAULT_CONNECTION_TIMEOUT, true);

            MqttMessage message = new MqttMessage(
                    payload != null ? payload.getBytes(StandardCharsets.UTF_8) : new byte[0]);
            message.setQos(qosLevel);

            socketHolder.getClient().publish(topic, message).waitForCompletion(PUBLISH_TIMEOUT_MS);

            String messageId = CentralConnectionManager.buildConnectionKey(brokerUrl, clientId)
                    + "::" + topic + "::" + System.currentTimeMillis();

            LOG.info("mqttPublish() successful: topic=" + topic + ", qos=" + qosLevel);
            return buildSuccessJson(topic, messageId);

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "mqttPublish() failed: topic=" + topic, e);
            return buildErrorJson(e.getMessage());
        }
    }

    private String buildSuccessJson(String topic, String messageId) {
        return "{\"status\":\"SUCCESS\",\"topic\":\"" + escapeJson(topic)
                + "\",\"messageId\":\"" + escapeJson(messageId) + "\"}";
    }

    private String buildErrorJson(String errorMessage) {
        return "{\"status\":\"ERROR\",\"errorMessage\":\"" + escapeJson(errorMessage) + "\"}";
    }

    private String escapeJson(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
